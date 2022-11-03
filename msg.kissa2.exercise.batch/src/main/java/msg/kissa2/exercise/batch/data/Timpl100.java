package msg.kissa2.exercise.batch.data;

import java.util.ArrayList;
import java.util.Arrays;

public class Timpl100 extends Table {
    public Timpl100 () {
        super();
    }

    @Override
    protected void buildStructure() {
        setName("Timpl100");
        schema = Arrays.asList(
                "KONTO",
                "ABETREUER",
                "STATUS",
                "IMPORTIERT",
                "KTOSALDO",
                "VERTR_DATAB",
                "VERTR_DATBI", "HK_NR", "SOH_SCHL",
                "BEW_DAR_BETR",
                "ZS",
                "EFF_ZS",
                "AUSZ_S",
                "TI_A",
                "ANNUITAET",
                "TI_TURN",
                "ZS_MM",
                "KOND_ZUS_DAT",
                "DARL_ART",
                "L_OPT_VLFZ",
                "L_OPT_LFZ",
                "FORWARD_KZ",
                "AUFL_KZ",
                "BGB_KZ",
                "SOTI_KZ",
                "SOTI_BETR",
                "SOTI_MIBETR",
                "SOTI_DATAB",
                "SOTI_DATBI",
                "SOTI_GEL",
                "SOTI_GEL_TILG",
                "SOTI_GEL_TILG_NB",
                "SOTI_TEIL",
                "SOTI_OPTPR",
                "VERTR_ENDE_KZ",
                "VERTR_BRUCH_KZ",
                "BETR_STUFE",
                "REG_AUSSCHL",
                "OFF_ZUSAGE",
                "POSNUMMER",
                "PRODUKTNR",
                "ANL_DAT"
        );
        records = new ArrayList<>();
    }
}
