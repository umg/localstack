package cloud.localstack;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.Record;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.internal.MultiFileOutputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import static com.fasterxml.jackson.databind.MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES;

/**
 * Simple implementation of a Java Lambda function executor.
 *
 * @author Waldemar Hummer
 */
public class LambdaExecutor {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: java " + LambdaExecutor.class.getSimpleName() +
                    " <lambdaClass> <recordsFilePath>");
            System.exit(1);
        }
        //switch off all logging
        LogManager.getLogManager().reset();
        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.OFF);

        String fileContent = readFile(args[1]);

        ObjectMapper reader = new ObjectMapper();
        reader.registerModule(new JodaModule());
        reader.configure(ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

        String handlerClassName = args[0];
        @SuppressWarnings("rawtypes")
        RequestHandler handler = ((Class<RequestHandler<?, ?>>) Class.forName(handlerClassName)).newInstance();
        Object event = mapEventFileToObject(fileContent, reader);


        Context ctx = new LambdaContext();
        Object result = handler.handleRequest(event, ctx);
        // The contract with lambci is to print the result to stdout, whereas logs go to stderr
        System.out.println(result);
    }

    @SuppressWarnings("deprecation")
    private static Object mapEventFileToObject(String messageContents, ObjectMapper mapper) throws Exception {
        if (messageContents.contains("\"Sns\"")) {
            return mapper.reader(SNSEvent.class).readValue(messageContents);
        } else if (messageContents.contains("\"Kinesis\"")) {
            return mapper.reader(KinesisEvent.class).readValue(messageContents);
        }

        throw new UnsupportedOperationException("Given event is neither an SNSEvent or a KinesisEvent");
    }

    private static <T> T get(Map<String, T> map, String key) {
        T result = map.get(key);
        if (result != null) {
            return result;
        }
        key = StringUtils.uncapitalize(key);
        result = map.get(key);
        if (result != null) {
            return result;
        }
        return map.get(key.toLowerCase());
    }

    private static String readFile(String file) throws Exception {
        if (!file.startsWith("/")) {
            file = System.getProperty("user.dir") + "/" + file;
        }
        return FileUtils.readFileToString(new File(file), Charsets.UTF_8);
    }

}
