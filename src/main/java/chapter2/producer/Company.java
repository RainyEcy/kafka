package chapter2.producer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class Company {
    private final String name;

    private final String address;

}
