package db.sql;

import java.util.List;
import java.util.ArrayList;
import org.antlr.v4.runtime.misc.NotNull;

public class CreateTableListener extends DiveBaseListener {
    private Schema schema = null;

    public Schema getSchema() { return schema; }

    @Override
    public void enterCreate_table_stmt(@NotNull DiveParser.Create_table_stmtContext ctx) {
        // Extract table name
        schema.setTableName(ctx.table_name().any_name().IDENTIFIER().getText());

        // Extract list of column definitions
        List<ColumnDescriptor> recordDescriptor = new ArrayList<ColumnDescriptor>();
        for (int i = 0; i < ctx.column_def().size(); i++) {
            String columnName = ctx.column_def(i).column_name().any_name().IDENTIFIER().getText();
            String typeName = ctx.column_def(i).type_name().name(0).any_name().IDENTIFIER().getText();
            // Convert type name to our own enum type
            FieldType fieldType;
            if (typeName.equals("int") || typeName.equals("smallint"))
                fieldType = FieldType.IntType;
            else if (typeName.equals("char") || typeName.equals("varchar"))
                fieldType = FieldType.StringType;
            recordDescriptor.add(new ColumnDescriptor(columnName, fieldType));
        }
        schema.setRecordDescriptor(recordDescriptor);
    }
}
