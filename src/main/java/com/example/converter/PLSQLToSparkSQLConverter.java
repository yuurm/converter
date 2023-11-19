package com.example.converter;



import java.io.*;
import java.util.regex.*;

public class PLSQLToSparkSQLConverter {

    public static void main(String[] args) throws IOException {
        // Чтение содержимого исходного файла PL/SQL
        String plsqlCode = readFileAsString("input.txt");

        // Преобразование PL/SQL кода в Spark SQL код
        String sparkSQLCode = convertPLSQLToSparkSQL(plsqlCode);

        // Запись преобразованного кода в конечный файл
        writeFile("output.txt", sparkSQLCode);
    }

    private static String convertPLSQLToSparkSQL(String plsqlCode) {

        // Регулярные выражения для поиска и замены соответствующих PL/SQL конструкций
        String toDatePattern = "(TO_DATE\\('.*?'\\,\\s*'.*?'\\))";
        String nvlPattern = "(NVL\\(.*?\\))";
        String roundPattern = "(ROUND\\(.*?\\))";
        String instrPattern = "(INSTR\\(.*?\\))";
        String casePattern = "(CASE\\s.*?END\\sCASE;)";
        String xmlAggPattern = "(XMLAGG\\(.*?\\))";
        String leadLagPattern = "(LEAD\\(.*?\\))|(LAG\\(.*?\\))";
        String rowsBetweenPattern = "(ROWS\\s*BETWEEN.*?\\))";
        String groupByPattern = "(GROUP\\s*BY\\s.*?\\))";
        String distinctPattern = "(DISTINCT\\s.*?\\))";
        String toCharPattern = "(TO_CHAR\\(.*?\\))";
        String continuePattern = "(CONTINUE;)";
        String dbmsOutputPattern = "(DBMS_OUTPUT\\.PUT_LINE\\(.*?\\))";
        String rownumPattern = "(ROWNUM)";
        String executeImmediatePattern = "(EXECUTE\\s*IMMEDIATE)";
        String regexpLikePattern = "(REGEXP_LIKE\\(.*?\\))";
        String datePartPattern = "(DATE_PART\\('.*?'\\))";
        String truncPattern = "(TRUNC\\(.*?\\))";
        String currentTimestampPattern = "(CURRENT_TIMESTAMP)";
        String monthsBetweenPattern = "(MONTHS_BETWEEN\\(.*?\\))";

        //
        // Регулярные выражения для поиска и замены соответствующих PL/SQL конструкций
        String ifThenElsePattern = "(IF\\s.*?THEN\\s.*?ELSE\\s.*?END\\sIF;)";
        String sumPattern = "(SUM\\(.*?\\))";
        String toNumberPattern = "(TO_NUMBER\\(.*?\\))";

        String sysdatePattern = "(SYSDATE)";
        String jsonValuePattern = "(JSON_VALUE\\(.*?\\))";

        String avgPattern = "(AVG\\(.*?\\))";



        // Замены для соответствующих PL/SQL конструкций
        String toDateReplacement = "Dataset<Row> result = spark.sql(\"SELECT to_date('01-01-2023', 'dd-MM-yyyy')\");";
        String nvlReplacement = "Dataset<Row> result = df.withColumn(\"new_column\", functions.when(df.col(\"column_name\").isNull(), \"default_value\").otherwise(df.col(\"column_name\")));";
        String roundReplacement = "Dataset<Row> result = df.withColumn(\"rounded_column\", functions.round(df.col(\"decimal_column\"), 2));";
        String instrReplacement = "Dataset<Row> result = df.withColumn(\"string_position\", functions.instr(df.col(\"string_column\"), \"search_string\"));";
        String caseReplacement = "Dataset<Row> result = df.withColumn(\"new_column\", functions.when(condition, value1).otherwise(value2));";
        String xmlAggReplacement = "Dataset<Row> result = df.selectExpr(\"xml_aggregate(xml('elem', column_name)) as xml_agg\");";
        String leadLagReplacement = "WindowSpec windowSpec = Window.orderBy(\"column_name\");\n" +
                "Dataset<Row> result = df.withColumn(\"lead_column\", functions.lead(df.col(\"column_name\")).over(windowSpec));";
        String rowsBetweenReplacement = "WindowSpec windowSpec = Window.orderBy(\"column_name\").rowsBetween(-1, 1);\n" +
                "Dataset<Row> result = df.withColumn(\"sum_column\", functions.sum(df.col(\"column_name\")).over(windowSpec));";
        String groupByReplacement = "Dataset<Row> result = df.groupBy(\"column_name\").agg(functions.sum(\"column2\"));";
        String distinctReplacement = "Dataset<Row> result = df.select(\"column_name\").distinct();";
        String toCharReplacement = "Dataset<Row> result = df.select(functions.date_format(df.col(\"date_column\"), \"dd-MMM-yyyy HH:mm:ss\"));";
        String continueReplacement = "// Применение циклов и условных операторов для управления потоком выполнения.";
        String dbmsOutputReplacement = "// Вывод логов в Spark SQL, например:\nSystem.out.println(\"Message\");";
        String rownumReplacement = "WindowSpec windowSpec = Window.orderBy(\"column_name\");\n" +
                "Dataset<Row> result = df.withColumn(\"row_num\", functions.row_number().over(windowSpec)).filter(df.col(\"row_num\").leq(10));";
        String executeImmediateReplacement = "// В Spark SQL и Java прямой эквивалент EXECUTE IMMEDIATE отсутствует.";
        String regexpLikeReplacement = "Dataset<Row> result = df.filter(df.col(\"column_name\").rlike(\"pattern\"));";
        String datePartReplacement = "Dataset<Row> result = df.select(functions.date_part(\"day\", df.col(\"date_column\")));";
        String truncReplacement = "Dataset<Row> result = df.select(functions.trunc(df.col(\"date_column\"), \"MM\"));";
        String currentTimestampReplacement = "Dataset<Row> result = spark.sql(\"SELECT current_timestamp()\");";
        String monthsBetweenReplacement = "Dataset<Row> result = df.select(functions.months_between(df.col(\"date1\"), df.col(\"date2\")));";

        // Замена PL/SQL конструкций на эквивалентные Spark SQL на Java
        plsqlCode = plsqlCode.replaceAll(toDatePattern, toDateReplacement);
        plsqlCode = plsqlCode.replaceAll(nvlPattern, nvlReplacement);
        plsqlCode = plsqlCode.replaceAll(roundPattern, roundReplacement);
        plsqlCode = plsqlCode.replaceAll(instrPattern, instrReplacement);
        plsqlCode = plsqlCode.replaceAll(casePattern, caseReplacement);
        plsqlCode = plsqlCode.replaceAll(xmlAggPattern, xmlAggReplacement);
        plsqlCode = plsqlCode.replaceAll(leadLagPattern, leadLagReplacement);
        plsqlCode = plsqlCode.replaceAll(rowsBetweenPattern, rowsBetweenReplacement);
        plsqlCode = plsqlCode.replaceAll(groupByPattern, groupByReplacement);
        plsqlCode = plsqlCode.replaceAll(distinctPattern, distinctReplacement);
        plsqlCode = plsqlCode.replaceAll(toCharPattern, toCharReplacement);
        plsqlCode = plsqlCode.replaceAll(continuePattern, continueReplacement);
        plsqlCode = plsqlCode.replaceAll(dbmsOutputPattern, dbmsOutputReplacement);
        plsqlCode = plsqlCode.replaceAll(rownumPattern, rownumReplacement);
        plsqlCode = plsqlCode.replaceAll(executeImmediatePattern, executeImmediateReplacement);
        plsqlCode = plsqlCode.replaceAll(regexpLikePattern, regexpLikeReplacement);
        plsqlCode = plsqlCode.replaceAll(datePartPattern, datePartReplacement);
        plsqlCode = plsqlCode.replaceAll(truncPattern, truncReplacement);
        plsqlCode = plsqlCode.replaceAll(currentTimestampPattern, currentTimestampReplacement);
        plsqlCode = plsqlCode.replaceAll(monthsBetweenPattern, monthsBetweenReplacement);





        // Замены для соответствующих PL/SQL конструкций
        String ifThenElseReplacement = "Column condition = df.col(\"condition\");\n" +
                "Column result = functions.when(condition, df.col(\"statements1\")).otherwise(df.col(\"statements2\"));\n" +
                "Dataset<Row> transformedDF = df.withColumn(\"result_column\", result);";
        String sumReplacement = "Dataset<Row> result = df.agg(functions.sum(\"column_name\"));";
        String toNumberReplacement = "Dataset<Row> result = df.withColumn(\"new_column\", df.col(\"column_name\").cast(DataTypes.DoubleType));";
        String sysdateReplacement = "java.util.Date sysdate = new java.util.Date();";
        String jsonValueReplacement = "Dataset<Row> result = df.withColumn(\"new_column\", functions.get_json_object(df.col(\"json_column\"), \"$\\.key\"));";

        String avgReplacement = "Dataset<Row> result = df.agg(functions.avg(\"column_name\"));";

        // Замена PL/SQL конструкций на эквивалентные Spark SQL на Java
        plsqlCode = plsqlCode.replaceAll(ifThenElsePattern, ifThenElseReplacement);
        plsqlCode = plsqlCode.replaceAll(sumPattern, sumReplacement);
        plsqlCode = plsqlCode.replaceAll(toNumberPattern, toNumberReplacement);
        plsqlCode = plsqlCode.replaceAll(nvlPattern, nvlReplacement);
        plsqlCode = plsqlCode.replaceAll(sysdatePattern, sysdateReplacement);
        plsqlCode = plsqlCode.replaceAll(jsonValuePattern, jsonValueReplacement);
        plsqlCode = plsqlCode.replaceAll(monthsBetweenPattern, monthsBetweenReplacement);
        plsqlCode = plsqlCode.replaceAll(toCharPattern, toCharReplacement);
        plsqlCode = plsqlCode.replaceAll(casePattern, caseReplacement);
        plsqlCode = plsqlCode.replaceAll(avgPattern, avgReplacement);

        //3
        // Регулярные выражения и замены для оператора LEAD()
        String leadPattern = "(LEAD\\(.*?\\))";
        String leadReplacement = "WindowSpec windowSpec = Window.orderBy(\"some_column\");\n" +
                "Dataset<Row> result = df.withColumn(\"next_value\", lead(df.col(\"column\"), 1).over(windowSpec));";

        // Регулярные выражения и замены для оператора LAG()
        String lagPattern = "(LAG\\(.*?\\))";
        String lagReplacement = "WindowSpec windowSpec = Window.orderBy(\"some_column\");\n" +
                "Dataset<Row> result = df.withColumn(\"previous_value\", lag(df.col(\"column\"), 1).over(windowSpec));";

        // Регулярные выражения и замены для оператора RANK()
        String rankPattern = "(RANK\\(\\))";
        String rankReplacement = "WindowSpec windowSpec = Window.partitionBy(\"column2\").orderBy(\"column3\");\n" +
                "Dataset<Row> result = df.withColumn(\"rank_column\", rank().over(windowSpec));";

        // Регулярные выражения и замены для оператора DENSE_RANK()
        String denseRankPattern = "(DENSE_RANK\\(\\))";
        String denseRankReplacement = "WindowSpec windowSpec = Window.orderBy(\"column2\");\n" +
                "Dataset<Row> result = df.withColumn(\"dense_rank_column\", dense_rank().over(windowSpec));";

        // Регулярные выражения и замены для оператора NTILE()
        String ntilePattern = "(NTILE\\(.*?\\))";
        String ntileReplacement = "WindowSpec windowSpec = Window.orderBy(\"some_column\");\n" +
                "Dataset<Row> result = df.withColumn(\"quartile\", ntile(4).over(windowSpec));";

        // Производим замены операторов в переданном PL/SQL коде
        plsqlCode = plsqlCode.replaceAll(leadPattern, leadReplacement);
        plsqlCode = plsqlCode.replaceAll(lagPattern, lagReplacement);
        plsqlCode = plsqlCode.replaceAll(rankPattern, rankReplacement);
        plsqlCode = plsqlCode.replaceAll(denseRankPattern, denseRankReplacement);
        plsqlCode = plsqlCode.replaceAll(ntilePattern, ntileReplacement);


        // Регулярные выражения и замены для оператора FIRST_VALUE()
        String firstValuePattern = "(FIRST_VALUE\\(.*?\\))";
        String firstValueReplacement = "WindowSpec windowSpec = Window.orderBy(\"column3\");\n" +
                "Dataset<Row> result = df.withColumn(\"first_value_column\", first(df.col(\"column2\")).over(windowSpec));";

        // Регулярные выражения и замены для оператора LAST_VALUE()
        String lastValuePattern = "(LAST_VALUE\\(.*?\\))";
        String lastValueReplacement = "WindowSpec windowSpec = Window.orderBy(\"column3\").rowsBetween(Window.unboundedPreceding(), Window.unboundedFollowing());\n" +
                "Dataset<Row> result = df.withColumn(\"last_value_column\", last(df.col(\"column2\"), true).over(windowSpec));";

        // Регулярные выражения и замены для оператора MEDIAN()
        String medianPattern = "(MEDIAN\\(.*?\\))";
        String medianReplacement = "WindowSpec windowSpec = Window.partitionBy(\"column3\").orderBy(\"column4\");\n" +
                "Dataset<Row> result = df.withColumn(\"median_column\", percentile_approx(df.col(\"column2\"), lit(0.5)).over(windowSpec));";

        // Регулярные выражения и замены для оператора PERCENTILE_CONT()
        String percentileContPattern = "(PERCENTILE_CONT\\(.*?\\))";
        String percentileContReplacement = "WindowSpec windowSpec = Window.partitionBy(\"column3\").orderBy(\"column2\");\n" +
                "Dataset<Row> result = df.withColumn(\"percentile_column\", expr(\"percentile(column2, 0.5)\").over(windowSpec));";

        // Регулярные выражения и замены для оператора PERCENTILE_DISC()
        String percentileDiscPattern = "(PERCENTILE_DISC\\(.*?\\))";
        String percentileDiscReplacement = "WindowSpec windowSpec = Window.partitionBy(\"column3\").orderBy(\"column2\");\n" +
                "Dataset<Row> result = df.withColumn(\"percentile_disc_column\", expr(\"percentile_disc(0.5) within group (order by column2)\").over(windowSpec));";

        // Регулярные выражения и замены для оператора WIDTH_BUCKET()
        String widthBucketPattern = "(WIDTH_BUCKET\\(.*?\\))";
        String widthBucketReplacement = "WindowSpec windowSpec = Window.orderBy(\"column3\");\n" +
                "Dataset<Row> result = df.withColumn(\"width_bucket_column\", expr(\"width_bucket(column2, 10, 100, 1000)\").over(windowSpec));";

        // Регулярные выражения и замены для оператора LISTAGG()
        String listaggPattern = "(LISTAGG\\(.*?\\))";
        String listaggReplacement = "WindowSpec windowSpec = Window.partitionBy(\"column1\").orderBy(\"column3\");\n" +
                "Dataset<Row> result = df.withColumn(\"listagg_column\", concat_ws(\",\", collect_list(df.col(\"column2\")).over(windowSpec)));";

        // Производим замены операторов в переданном PL/SQL коде
        plsqlCode = plsqlCode.replaceAll(firstValuePattern, firstValueReplacement);
        plsqlCode = plsqlCode.replaceAll(lastValuePattern, lastValueReplacement);
        plsqlCode = plsqlCode.replaceAll(medianPattern, medianReplacement);
        plsqlCode = plsqlCode.replaceAll(percentileContPattern, percentileContReplacement);
        plsqlCode = plsqlCode.replaceAll(percentileDiscPattern, percentileDiscReplacement);
        plsqlCode = plsqlCode.replaceAll(widthBucketPattern, widthBucketReplacement);
        plsqlCode = plsqlCode.replaceAll(listaggPattern, listaggReplacement);




        return plsqlCode;
    }

    private static String readFileAsString(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line).append("\n");
        }
        reader.close();
        return stringBuilder.toString();
    }

    private static void writeFile(String filePath, String content) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
        writer.write(content);
        writer.close();
    }
}

