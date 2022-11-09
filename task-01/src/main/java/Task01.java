import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class Task01 {

    @Getter
    @Setter
    static class Survey {

        @Getter
        @Setter
        static class LivingPlace {
            private String city;
            private String province;
        }

        @Getter
        @Setter
        static class Skill {
            private String name;
            private String description;
            private int level;
        }

        private String name;
        private String surname;
        private int age;
        private LivingPlace livingPlace;
        private List<Skill> skills;

        public Survey() {
            this.skills = new ArrayList<>();
            this.livingPlace = new LivingPlace();
        }

    }

    public static void main(String[] args) {

        try {

            System.out.println("Open connection on HDFS...");

            // My master vm exposes its HDFS port on 9000 in the current client machine
            FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), new Configuration());

            System.out.println("Connection opened.");
            System.out.println();

            List<Survey> surveys = new ArrayList<>();

            do {

                System.out.println("Insert data of the employee (leave empty to exit and save): ");

                Survey survey = new Survey();

                System.out.print("Name: ");
                survey.setName(new Scanner(System.in).nextLine());

                if(survey.getName().isEmpty()) {
                    break;
                }

                System.out.print("Surname: ");
                survey.setSurname(new Scanner(System.in).nextLine());

                System.out.print("Age: ");
                survey.setAge(new Scanner(System.in).nextInt());

                System.out.println("Living Place: ");

                System.out.print(" - City: ");
                survey.getLivingPlace().setCity(new Scanner(System.in).nextLine());

                System.out.print(" - Province: ");
                survey.getLivingPlace().setProvince(new Scanner(System.in).nextLine());

                System.out.println("Programming Skills (leave blank to finish): ");

                do {

                    Survey.Skill skill = new Survey.Skill();

                    System.out.print(" - Name: ");
                    skill.setName(new Scanner(System.in).nextLine());

                    if (skill.getName().isEmpty()) {
                        break;
                    }

                    System.out.print(" - Description: ");
                    skill.setDescription(new Scanner(System.in).nextLine());

                    System.out.print(" - Level: ");
                    skill.setLevel(new Scanner(System.in).nextInt());

                    survey.getSkills().add(skill);

                } while(true);

                surveys.add(survey);

            } while (true);

            System.out.println("Saving data on HDFS...");

            if(!hdfs.exists(new Path("/user/root/surveys"))) {
                hdfs.mkdirs(new Path("/user/root/surveys"));
            }

            Path path;
            do {
                path = new Path("/user/root/surveys/survey-" + UUID.randomUUID() + ".json");
            } while (hdfs.exists(path));

            try(FSDataOutputStream stream = hdfs.create(path, false)) {

                for(Survey survey : surveys) {

                    stream.writeBytes(new GsonBuilder()
                            .create()
                            .toJson(survey));

                    stream.writeUTF("\n");

                }

                System.out.println("Data saved successfully on HDFS: " + path);

            } catch (IOException e) {
                System.out.println("Cannot save data on HDFS: " + e.getMessage());
            }

        } catch (IOException e) {
            System.out.println("Connection failed: " + e.getMessage());
        }

    }

}
