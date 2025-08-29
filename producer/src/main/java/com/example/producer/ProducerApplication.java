package com.example.producer;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannelSpec;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.File;
import java.util.Map;

@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

}

// "enterprise integration patterns" by gregor hohpe and bobby woolf

@Component
class Uppercaser {

    String uppercase(String s) {
        return s.toUpperCase();
    }
}


@Controller
@ResponseBody
class HttpFlowLaunchingThingController {

    private final UppercaserGateway gateway;

    HttpFlowLaunchingThingController(UppercaserGateway gateway) {
        this.gateway = gateway;
    }

    @GetMapping("/go")
    String send(@RequestParam String s) {
        return this.gateway.uppercase(s);
    }

}

@MessagingGateway
interface UppercaserGateway {

    @Gateway(requestChannel = FileIntegrationFlow.UPPERCASE_CHANNEL_REQUESTS)
    String uppercase(@Payload String message);
}

@IntegrationComponentScan
@Configuration
class FileIntegrationFlow {


    @Bean
    IntegrationFlow filesToConsoleIntegrationFlow(
            @Value("${HOME}/Desktop/in") File inbound,
            @Qualifier(UPPERCASE_CHANNEL_REQUESTS) MessageChannel uppercaseChannel
    ) {
        var files = Files
                .inboundAdapter(inbound)
                .autoCreateDirectory(true);
        return IntegrationFlow
                .from(files)
                .transform(new FileToStringTransformer())
                .channel(uppercaseChannel)
                .get();
    }


    static final String UPPERCASE_CHANNEL_REQUESTS = "uppercaseRequests";
    static final String UPPERCASE_CHANNEL_REPLIES = "uppercaseReplies";

    @Bean(UPPERCASE_CHANNEL_REPLIES)
    MessageChannelSpec<DirectChannelSpec, DirectChannel> replies() {
        return MessageChannels.direct();
    }

    @Bean(UPPERCASE_CHANNEL_REQUESTS)
    MessageChannelSpec<DirectChannelSpec, DirectChannel> requests() {
        return MessageChannels.direct();
    }

    @Bean
    IntegrationFlow uppercaseToConsoleIntegrationFlow(
            Uppercaser uppercaser,
            @Qualifier(UPPERCASE_CHANNEL_REQUESTS) MessageChannel uppercaseChannel,
            MessageLogger messageLogger
    ) {
        // inbound adapter

        // push
        //        IMAP
        //        POP
        // poll
//

        // outbound adapter
        return IntegrationFlow
                .from(uppercaseChannel)
//                .transform(new FileToStringTransformer())
                .transform(uppercaser::uppercase)
                .handle((payload, headers) ->
                        MessageBuilder.withPayload(payload)
                                .setHeader("foo", "bar")
                                .copyHeadersIfAbsent(headers)
                                .build())
                .handle((p, h) -> {
                    messageLogger.log(p, h);
                    return p  ;
                })
             //   .channel(UPPERCASE_CHANNEL_REPLIES)
//				.handle()
//				.split()
//				.aggregate()
//				.route()
//				.filter()
                .get();
    }


    // coupling: location, payload, time

    /*
     - messaging
     - file synchronization
     - shared database
     - rpc
     */

}

@Component
class MessageLogger {

    void log(Object payload, Map<String, Object> headers) {
        System.out.println("payload: " + payload);
        headers.forEach((k, v) -> System.out.println("k: " + k + ", v: " + v));
    }
}