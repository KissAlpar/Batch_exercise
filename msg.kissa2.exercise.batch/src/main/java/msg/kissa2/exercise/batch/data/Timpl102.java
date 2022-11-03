package msg.kissa2.exercise.batch.data;

import java.util.ArrayList;
import java.util.Arrays;

public class Timpl102 extends Table {

    public Timpl102 () {
        super();
    }

    @Override
    protected void buildStructure() {
        setName("Timpl102");
        schema = Arrays.asList(
                "LFD_NR",
                "KONTO",
                "STUFE",
                "DATUM",
                "STATUS",
                "BETRAG",
                "TYP",
                "MANUELL_KZ",
                "LFD_NR_REF"
        );
        records = new ArrayList<>();
    }
}
