package no.safebase.avro.specific;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {
    public static void main(String[] args) {

        // step 1: create a specific record
        final Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("John");
        customerBuilder.setLastName("Doe");
        customerBuilder.setWeight(85.5f);
        customerBuilder.setHeight(186.5f);
        customerBuilder.setAutomatedEmail(false);

        final Customer customer = customerBuilder.build();

        // step 2: write that specific record to a file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("avro-examples/customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("Written customer-specific.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }
        // step 3: read a specific record from a file
        final File file = new File("avro-examples/customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        try (DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                Customer customerRead = dataFileReader.next();
                System.out.println(customerRead.toString());
                System.out.println("First name: " + customerRead.getFirstName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
