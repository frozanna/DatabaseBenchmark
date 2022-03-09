package DatabaseAdapters;

import java.time.LocalDate;
import java.sql.Date;

public class AdapterUtils {
    public static Date localdateToDate(LocalDate date){
        return java.sql.Date.valueOf(date);
    }
}
