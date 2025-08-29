package com.example.graalvm;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotContribution;
import org.springframework.beans.factory.aot.BeanFactoryInitializationAotProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashSet;

//@RegisterReflectionForBinding({Cat.class})
@SpringBootApplication
@ImportRuntimeHints(GraalvmApplication.Hints.class)
public class GraalvmApplication {

    public static void main(String[] args) {
        SpringApplication.run(GraalvmApplication.class, args);
    }

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            hints.resources().registerResource(HELLO_RESOURCE);
            hints.reflection().registerType(Cat.class, MemberCategory.values());
        }
    }

    static final Resource HELLO_RESOURCE = new ClassPathResource("hello");

    @Bean
    static SerializableBeanFactoryInitializationAotProcessor garyBeanFactoryInitializationAotProcessor() {
        return new SerializableBeanFactoryInitializationAotProcessor();
    }

    @Bean
    ApplicationRunner messageRunner() {
        return _ -> System.out.println(HELLO_RESOURCE.getContentAsString(Charset.defaultCharset()));
    }
}

@Component
class Cart implements Serializable {
}

class SerializableBeanFactoryInitializationAotProcessor
        implements BeanFactoryInitializationAotProcessor {

    @Override
    public BeanFactoryInitializationAotContribution processAheadOfTime(
            ConfigurableListableBeanFactory beanFactory) {

        var classesThatNeedToBeRegisteredForSerialization = new HashSet<Class<?>>();
        var names = beanFactory.getBeanDefinitionNames();
        for (var beanName : names) {
//            var beanDefinition = beanFactory.getBeanDefinition(beanName);
            var type = beanFactory.getType(beanName);
            System.out.println("inspecting bean " + beanName + " with type " + type);
            if (Serializable.class.isAssignableFrom( type)) {
                classesThatNeedToBeRegisteredForSerialization.add(type);
            }
        }
        return (generationContext, beanFactoryInitializationCode) -> {
            var runtimeHints = generationContext.getRuntimeHints();
            for (var clazz : classesThatNeedToBeRegisteredForSerialization) {
                runtimeHints.serialization().registerType(TypeReference.of(clazz.getName()));
                System.out.println("registering " + clazz.getName() + " for serialization");
            }
        };
    }
}

/*
@Controller
@ResponseBody
class CatController {

    @GetMapping("/cat")
    Cat cat() {
        return new Cat("Felix");
    }
}

*/

@Component
class Demo implements ApplicationRunner {

    private final ObjectMapper mapper;

    Demo(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        var felix = new Cat("Felix");
        var json = this.mapper.writeValueAsString(felix);
        System.out.println(json);

        // 1.
        var maybe = Math.random() > 0 ? "" : "";
        var aClass = Class.forName("com.example" + maybe +
                ".graalvm.Cat");
        var ctor = aClass.getDeclaredConstructor(String.class);
        var instanceOfCat = (Cat) ctor.newInstance("Felix");
        System.out.println(instanceOfCat.name());

    }
}

record Cat(String name) {
}

// 1. reflection
// 2. resources
// 3. serialization
// 4. proxies
// 5. JNI