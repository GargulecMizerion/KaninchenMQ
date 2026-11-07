# Kontrakt między serwerem TCP a rdzeniem KaninchenMQ

Propozycja do wspólnego review. Opisuje komunikację między warstwami dla wszystkich poleceń planowanego protokołu TCP. Obecny serwer odpowiada echem. Sygnatury i przykłady poniżej wyznaczają sposób przejścia od echa do obsługi brokera.

## Granica między warstwami

```text
Klient → ramka JSON przez TCP → dekoder Netty → ClientCommand
       → ConnectionHandler → metoda Broker → wynik lub błąd
       → ConnectionHandler → ramka odpowiedzi przez TCP
```

**Serwer (osoba A)** składa ramki, odczytuje JSON, sprawdza wymagane pola i ich typy oraz tworzy rekord polecenia, np. `PublishCommand`. Handler wybiera metodę `Broker`, przekazuje argumenty Javy i zamienia wynik na ramkę odpowiedzi. `type` i `requestId` obsługuje warstwa protokołu. `connectionId` nadany przez serwer pełni rolę `sessionId` przy operacjach konsumenta.

**Rdzeń (osoba B)** sprawdza stan brokera i wykonuje operację. Zarządza kolejkami, exchange’ami, bindingami, routingiem, subskrypcjami, ACK/NACK i zapisem stanu. Przyjmuje typy Javy i zwraca wynik domenowy albo błąd z kodem, np. `QUEUE_NOT_FOUND`.

**Uruchomienie.** `Main` tworzy jedną instancję implementacji `Broker` i przekazuje ją do `BrokerServer` oraz REST API. `BrokerServer` podaje ją każdemu `ConnectionHandler` wraz z identyfikatorem połączenia. REST API wywołuje te same metody bezpośrednio w Javie.

## Wspólny interfejs w Javie

Interfejs i typy użyte w jego sygnaturach mogą znaleźć się w pakiecie `tu.kaninchen.mq.broker.api`.

```java
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface Broker {
    CompletionStage<Void> declareQueue(String queue);
    CompletionStage<Void> deleteQueue(String queue);
    CompletionStage<Void> declareExchange(String exchange, ExchangeType type);
    CompletionStage<Void> deleteExchange(String exchange);
    CompletionStage<Void> bind(String exchange, String queue, String bindingKey);

    CompletionStage<PublishResult> publish(String exchange, String routingKey, byte[] body);

    CompletionStage<Void> subscribe(
            String sessionId, String subscriptionId, String queue,
            int prefetch, Consumer<Delivery> onDelivery);

    CompletionStage<Void> ack(String sessionId, long deliveryTag);
    CompletionStage<Void> nack(String sessionId, long deliveryTag, boolean requeue);
    CompletionStage<Void> closeSession(String sessionId);
}
```

Typy kontraktu to `ExchangeType` z wartościami `DIRECT`, `TOPIC`, `FANOUT`, `PublishResult(int routedQueues)` oraz `Delivery(long deliveryTag, byte[] body)`. Ten ostatni typ opisuje jedno dostarczenie. Handler zna identyfikator subskrypcji z rejestracji funkcji `onDelivery`.

Metody przyjmują operację i szybko zwracają `CompletionStage`. Osoba B może zacząć od jednego osobnego wątku wykonującego operacje rdzenia w kolejności przyjęcia. Netty przekazuje polecenia z danego połączenia w kolejności ich odczytania i rejestruje obsługę wyniku przez `whenComplete`. Po zakończeniu operacji handler zleca zapis odpowiedzi przez `ctx.executor().execute(...)`. Dzięki temu operacja dyskowa może trwać, podczas gdy Netty obsługuje pozostałe połączenia.

Po dodaniu trwałości operacje zmieniające trwały stan kończą się sukcesem po zapisaniu zmiany na dysku. Po restarcie rdzeń odtwarza kolejki, exchange’e, bindingi i wiadomości oczekujące, w tym dostarczenia przerwane przed potwierdzeniem. Klient po ponownym połączeniu rejestruje subskrypcje od nowa.

## Ramki i odpowiedzi

Każda ramka to jeden obiekt JSON w UTF-8 zakończony znakiem nowej linii. Przykłady pokazują zawartość linii. Serwer stosuje limit długości ramki. Klient nadaje `requestId` unikalny wśród swoich oczekujących żądań na danym połączeniu.

- **Sukces** daje `OK` z `requestId` i ewentualnym wynikiem. `PING` daje `PONG`.
- **Błąd** daje `ERROR` z `requestId` i kodem `code`. Serwer używa `BAD_REQUEST` przy niepoprawnym formacie, brakującym polu albo nieznanym typie. Gdy z ramki nie da się odczytać identyfikatora, odpowiedź używa `"requestId":null`. Dla poprawnej ramki odrzuconej przez rdzeń handler mapuje kod błędu domenowego, a nieoczekiwany błąd mapuje na `INTERNAL_ERROR`.
- **Dostarczenie** to osobna ramka `DELIVERY`, którą identyfikują `subscriptionId` i `deliveryTag`.

```json
{"type":"OK","requestId":"q1"}
{"type":"ERROR","requestId":"p1","code":"EXCHANGE_NOT_FOUND"}
```

## Polecenia klienta

### PING

```json
{"type":"PING","requestId":"ping1"}
```

**Serwer** odpowiada `{"type":"PONG","requestId":"ping1"}` bezpośrednio z handlera. Pozwala to sprawdzić połączenie, dekoder i kodowanie odpowiedzi jeszcze przed podpięciem rdzenia.

### DECLARE_QUEUE

```json
{"type":"DECLARE_QUEUE","requestId":"q1","queue":"orders.created"}
```

**Serwer** wywołuje `broker.declareQueue("orders.created")`. **Rdzeń** tworzy kolejkę, a powtórną deklarację tej samej kolejki traktuje jako sukces. Handler zwraca `OK` po zakończeniu operacji.

### DELETE_QUEUE

```json
{"type":"DELETE_QUEUE","requestId":"q2","queue":"orders.created"}
```

**Serwer** wywołuje `broker.deleteQueue("orders.created")`. **Rdzeń** usuwa pustą kolejkę bez subskrypcji i niepotwierdzonych dostarczeń, razem z jej bindingami. Handler zwraca `OK`, `QUEUE_NOT_FOUND` albo `QUEUE_IN_USE`, jeśli kolejka ma wiadomości, aktywne subskrypcje lub dostarczenia oczekujące na potwierdzenie.

### DECLARE_EXCHANGE

```json
{"type":"DECLARE_EXCHANGE","requestId":"e1","exchange":"orders","exchangeType":"DIRECT"}
```

**Serwer** zamienia `exchangeType` na enum i wywołuje `broker.declareExchange("orders", ExchangeType.DIRECT)`. **Rdzeń** tworzy exchange. Powtórna deklaracja tej samej nazwy z tym samym typem daje `OK`, a próba deklaracji tej nazwy z innym typem daje `EXCHANGE_TYPE_MISMATCH`.

### DELETE_EXCHANGE

```json
{"type":"DELETE_EXCHANGE","requestId":"e2","exchange":"orders"}
```

**Serwer** wywołuje `broker.deleteExchange("orders")`. **Rdzeń** usuwa exchange wraz z jego bindingami. Handler zwraca `OK` albo `EXCHANGE_NOT_FOUND`.

### BIND

```json
{"type":"BIND","requestId":"b1","exchange":"orders","queue":"orders.created","bindingKey":"order.created"}
```

**Serwer** wywołuje `broker.bind("orders", "orders.created", "order.created")`. **Rdzeń** sprawdza istnienie exchange’a i kolejki oraz zapisuje binding. Powtórzenie identycznego bindingu daje `OK`. Brak zasobu daje odpowiednio `EXCHANGE_NOT_FOUND` lub `QUEUE_NOT_FOUND`. Przy `FANOUT` rdzeń kieruje wiadomość do wszystkich powiązanych kolejek niezależnie od klucza. Klient może wtedy użyć pustego `bindingKey`.

### PUBLISH

```json
{"type":"PUBLISH","requestId":"p1","exchange":"orders","routingKey":"order.created","bodyBase64":"SGVq"}
```

**Serwer** dekoduje `bodyBase64` do `byte[]` i wywołuje `broker.publish("orders", "order.created", body)`. **Rdzeń** dobiera kolejki według typu exchange’a i bindingów. Każda dopasowana kolejka dostaje jedną kopię wiadomości, także wtedy, gdy pasuje kilka jej bindingów.

Handler zwraca `{"type":"OK","requestId":"p1","routedQueues":1}` dla jednej dopasowanej kolejki. Istniejący exchange bez pasujących kolejek daje wynik `0`, a brak exchange’a daje `EXCHANGE_NOT_FOUND`. Gdy trwałość jest dostępna, rdzeń kończy udaną publikację do kolejek dopiero po trwałym zapisie.

### SUBSCRIBE

```json
{"type":"SUBSCRIBE","requestId":"s1","queue":"orders.created","subscriptionId":"orders-reader","prefetch":1}
```

**Serwer** wywołuje `broker.subscribe(connectionId, "orders-reader", "orders.created", 1, onDelivery)`. Funkcja `onDelivery` zleca Netty wysłanie ramki `DELIVERY` na tym połączeniu i szybko wraca. **Rdzeń** rejestruje subskrypcję, pilnuje unikalności `subscriptionId` w sesji i stosuje `prefetch`, czyli limit dostarczeń oczekujących na ACK lub NACK w tej subskrypcji.

Handler zwraca `OK` po rejestracji albo `QUEUE_NOT_FOUND`, `SUBSCRIPTION_EXISTS` lub `INVALID_PREFETCH`, jeśli `prefetch` jest mniejsze od 1. Rdzeń może wywołać `onDelivery` zaraz po zarejestrowaniu subskrypcji. Klient rejestruje odbiorcę dla swojego `subscriptionId` przed wysłaniem `SUBSCRIBE`, żeby odebrać również `DELIVERY`, które dotrze przed `OK`.

```json
{"type":"DELIVERY","subscriptionId":"orders-reader","deliveryTag":17,"bodyBase64":"SGVq"}
```

### ACK

```json
{"type":"ACK","requestId":"a1","deliveryTag":17}
```

**Serwer** wywołuje `broker.ack(connectionId, 17)`. **Rdzeń** wyszukuje aktywne dostarczenie w tej sesji, uznaje wiadomość za przetworzoną i zwalnia miejsce w limicie `prefetch`. Handler zwraca `OK` albo `DELIVERY_NOT_FOUND`, jeśli podany tag nie wskazuje aktywnego dostarczenia w tej sesji.

### NACK

```json
{"type":"NACK","requestId":"n1","deliveryTag":17,"requeue":true}
```

**Serwer** wywołuje `broker.nack(connectionId, 17, true)`. **Rdzeń** kończy próbę dostarczenia i zwalnia miejsce w limicie `prefetch`. Przy `requeue=true` zwraca wiadomość do kolejki, a przy `requeue=false` usuwa ją z kolejki. Ponowne dostarczenie dostaje tag nowej próby w sesji odbiorcy. Handler zwraca `OK` albo `DELIVERY_NOT_FOUND` według tych samych zasad co dla `ACK`.

## Rozłączenie klienta

Netty wywołuje `channelInactive`, a handler przekazuje `broker.closeSession(connectionId)`. Rdzeń usuwa subskrypcje sesji i zwraca jej niepotwierdzone wiadomości do kolejek. Powtórne zamknięcie sesji jest bezpieczne. Jeśli zapis ramki `DELIVERY` się nie powiedzie, serwer zamyka połączenie, żeby użyć tego samego mechanizmu odzyskania wiadomości.

## Praca równoległa i integracja

1. Wspólnie uzgodnijcie interfejs, typy wyników i kody błędów z tego pliku.
2. Osoba A używa atrapy `Broker` w `EmbeddedChannel`. Sprawdza, czy ramka wywołuje właściwą metodę, czy odpowiedź zachowuje `requestId` i czy wywołanie `onDelivery` daje ramkę `DELIVERY`.
3. Osoba B testuje rdzeń bezpośrednio w JUnit. Sprawdza routing, limit `prefetch`, ACK/NACK i powrót wiadomości do kolejki po zamknięciu sesji.
4. Pierwszy wspólny test uruchamia Netty z prawdziwym rdzeniem. Klient deklaruje exchange i kolejkę, tworzy binding, publikuje wiadomość i odbiera ją. Rozłącza się przed ACK. Drugi konsument odbiera tę wiadomość i potwierdza dostarczenie.
