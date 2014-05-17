package db.sql;

import java.io.IOException;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.LexerInterpreter;
import org.antlr.v4.runtime.ParserInterpreter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.tool.Grammar;

import db.syntax.DiveLexer;
import db.syntax.DiveParser;

public class SQLParser {
	public static ParseTree parse(String fileName,
			String combinedGrammarFileName, String startRule)
			throws IOException {
		final Grammar g = Grammar.load(combinedGrammarFileName);
		LexerInterpreter lexEngine = g
				.createLexerInterpreter(new ANTLRFileStream(fileName));
		CommonTokenStream tokens = new CommonTokenStream(lexEngine);
		ParserInterpreter parser = g.createParserInterpreter(tokens);
		ParseTree t = parser.parse(g.getRule(startRule).index);
		//String[] rules = parser.getRuleNames();
		//System.out.println("parse tree: " + t.toStringTree(parser));
		return t;
	}

	
	public static void main(String args[]) throws IOException {
		DiveLexer lexer = new DiveLexer(new ANTLRFileStream("data/sql"));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		DiveParser parser = new DiveParser(tokens);
		ParseTree tree = parser.parse(); // launch our special version of the parser

		
		//ParseTree tree = DiveParser.parse("data/sql", "data/Dive.g4", "parse");
		ParseTreeWalker walker = new ParseTreeWalker();
		DivePlanListener listener = new DivePlanListener();
		walker.walk(listener, tree);
		for(String table: listener.tables) {
			System.out.println(table);
		}
	}
}
