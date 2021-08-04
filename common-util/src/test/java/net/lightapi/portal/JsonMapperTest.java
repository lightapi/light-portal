package net.lightapi.portal;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JsonMapperTest {
    @Test
    public void testStringList() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String s = "[\"microservices\"]";
        List<String> list = objectMapper.readValue(s, new TypeReference<>(){});
        System.out.println(list.size());
        System.out.println(list.get(0));
    }

}
