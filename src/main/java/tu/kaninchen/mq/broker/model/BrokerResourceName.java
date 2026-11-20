package tu.kaninchen.mq.broker.model;

import tu.kaninchen.mq.broker.exception.BrokerException;

import java.util.regex.Pattern;

public record BrokerResourceName(String value) {

    private static final Pattern VALID_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9]*([._-][a-zA-Z0-9]+)*$");
    private static final int MAX_LENGTH = 128;

    public BrokerResourceName {
        if (value == null) {
            throw new NullPointerException();
        }
        if (value.length() > MAX_LENGTH) {
            throw new BrokerException(BrokerException.Code.INVALID_RESOURCE_NAME_LENGTH,
                "Resource name exceeds maximum length of " + MAX_LENGTH + " characters");
        }
        if (!VALID_PATTERN.matcher(value).matches()) {
            throw new BrokerException(BrokerException.Code.INVALID_RESOURCE_NAME, "Resource name has invalid format");
        }
    }
}
