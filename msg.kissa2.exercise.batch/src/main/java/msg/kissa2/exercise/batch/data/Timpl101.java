package msg.kissa2.exercise.batch.data;

import java.util.ArrayList;
import java.util.Arrays;

public class Timpl101 extends Table {

    public Timpl101 () {
        super();
    }

    @Override
    protected void buildStructure() {
        setName("Timpl101");
        schema = Arrays.asList(
                "KONTO",
                "STATUS",
                "STUFE",
                "DATUM_VON",
                "DATUM_BIS",
                "BETRAG_OPT",
                "BETRAG_SOTI",
                "BETRAG_SOTI_NB",
                "BETRAG_OPT_KOR",
                "TRANCHE",
                "SDIS_WERT"
        );
        records = new ArrayList<>();
    }
}
