cd data
java -cp ".:../lib/antlr-4.2.2-complete.jar" org.antlr.v4.Tool -visitor Dive.g4  -package db.syntax -o ../src/db/syntax

