package msg.kissa2.exercise.batch.data;

import jdk.nashorn.internal.objects.annotations.Constructor;
import lombok.Data;

import java.util.List;

@Data

public class CsvRecord {
    List<String> list;


    public CsvRecord(List<String> list) {
        this.list = list;
    }

}
