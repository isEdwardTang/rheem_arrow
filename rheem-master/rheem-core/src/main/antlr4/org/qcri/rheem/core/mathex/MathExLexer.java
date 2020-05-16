// Generated from /Users/edward/Code/Lab/rheem_arrow/rheem-master/rheem-core/src/main/antlr4/org/qcri/rheem/core/mathex/MathEx.g4 by ANTLR 4.8
package org.qcri.rheem.core.mathex;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class MathExLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, NUMBER=9, 
		WS=10, PREC0_OP=11, PREC1_OP=12, PREC2_OP=13, IDENTIFIER=14;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "CHAR", 
			"DIGIT", "INT", "EXP", "NUMBER", "WS", "PREC0_OP", "PREC1_OP", "PREC2_OP", 
			"IDENTIFIER", "IDENTIFIER_START", "IDENTIFIER_MIDDLE", "IDENTIFIER_END"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "'-'", "'+'", "'*'", "'%'", "'/'", "','", null, null, 
			null, null, "'^'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, "NUMBER", "WS", 
			"PREC0_OP", "PREC1_OP", "PREC2_OP", "IDENTIFIER"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public MathExLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "MathEx.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

//	@Override
//	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\20\u0090\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\3\2\3\2\3\3\3\3\3\4\3\4"+
		"\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\7\f"+
		"D\n\f\f\f\16\fG\13\f\3\f\5\fJ\n\f\3\r\3\r\5\rN\n\r\3\r\3\r\3\16\5\16S"+
		"\n\16\3\16\5\16V\n\16\3\16\3\16\6\16Z\n\16\r\16\16\16[\3\16\5\16_\n\16"+
		"\3\16\5\16b\n\16\3\16\3\16\3\16\3\16\5\16h\n\16\3\16\5\16k\n\16\3\17\6"+
		"\17n\n\17\r\17\16\17o\3\17\3\17\3\20\3\20\3\21\3\21\3\22\3\22\3\23\3\23"+
		"\7\23|\n\23\f\23\16\23\177\13\23\3\23\5\23\u0082\n\23\3\24\5\24\u0085"+
		"\n\24\3\25\3\25\3\25\5\25\u008a\n\25\3\26\3\26\3\26\5\26\u008f\n\26\2"+
		"\2\27\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\2\25\2\27\2\31\2\33\13\35"+
		"\f\37\r!\16#\17%\20\'\2)\2+\2\3\2\13\4\2C\\c|\3\2\62;\3\2\63;\4\2GGgg"+
		"\4\2--//\5\2\13\f\17\17\"\"\5\2\'\',,\61\61\5\2C\\aac|\4\2\60\60aa\2\u009a"+
		"\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2"+
		"\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2"+
		"\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\3-\3\2\2\2\5/\3\2\2\2\7\61\3\2\2\2\t"+
		"\63\3\2\2\2\13\65\3\2\2\2\r\67\3\2\2\2\179\3\2\2\2\21;\3\2\2\2\23=\3\2"+
		"\2\2\25?\3\2\2\2\27I\3\2\2\2\31K\3\2\2\2\33j\3\2\2\2\35m\3\2\2\2\37s\3"+
		"\2\2\2!u\3\2\2\2#w\3\2\2\2%y\3\2\2\2\'\u0084\3\2\2\2)\u0089\3\2\2\2+\u008e"+
		"\3\2\2\2-.\7*\2\2.\4\3\2\2\2/\60\7+\2\2\60\6\3\2\2\2\61\62\7/\2\2\62\b"+
		"\3\2\2\2\63\64\7-\2\2\64\n\3\2\2\2\65\66\7,\2\2\66\f\3\2\2\2\678\7\'\2"+
		"\28\16\3\2\2\29:\7\61\2\2:\20\3\2\2\2;<\7.\2\2<\22\3\2\2\2=>\t\2\2\2>"+
		"\24\3\2\2\2?@\t\3\2\2@\26\3\2\2\2AE\t\4\2\2BD\5\25\13\2CB\3\2\2\2DG\3"+
		"\2\2\2EC\3\2\2\2EF\3\2\2\2FJ\3\2\2\2GE\3\2\2\2HJ\5\25\13\2IA\3\2\2\2I"+
		"H\3\2\2\2J\30\3\2\2\2KM\t\5\2\2LN\t\6\2\2ML\3\2\2\2MN\3\2\2\2NO\3\2\2"+
		"\2OP\5\27\f\2P\32\3\2\2\2QS\7/\2\2RQ\3\2\2\2RS\3\2\2\2SU\3\2\2\2TV\5\27"+
		"\f\2UT\3\2\2\2UV\3\2\2\2VW\3\2\2\2WY\7\60\2\2XZ\t\3\2\2YX\3\2\2\2Z[\3"+
		"\2\2\2[Y\3\2\2\2[\\\3\2\2\2\\^\3\2\2\2]_\5\31\r\2^]\3\2\2\2^_\3\2\2\2"+
		"_k\3\2\2\2`b\7/\2\2a`\3\2\2\2ab\3\2\2\2bc\3\2\2\2cd\5\27\f\2de\5\31\r"+
		"\2ek\3\2\2\2fh\7/\2\2gf\3\2\2\2gh\3\2\2\2hi\3\2\2\2ik\5\27\f\2jR\3\2\2"+
		"\2ja\3\2\2\2jg\3\2\2\2k\34\3\2\2\2ln\t\7\2\2ml\3\2\2\2no\3\2\2\2om\3\2"+
		"\2\2op\3\2\2\2pq\3\2\2\2qr\b\17\2\2r\36\3\2\2\2st\t\6\2\2t \3\2\2\2uv"+
		"\t\b\2\2v\"\3\2\2\2wx\7`\2\2x$\3\2\2\2y\u0081\5\'\24\2z|\5)\25\2{z\3\2"+
		"\2\2|\177\3\2\2\2}{\3\2\2\2}~\3\2\2\2~\u0080\3\2\2\2\177}\3\2\2\2\u0080"+
		"\u0082\5+\26\2\u0081}\3\2\2\2\u0081\u0082\3\2\2\2\u0082&\3\2\2\2\u0083"+
		"\u0085\t\t\2\2\u0084\u0083\3\2\2\2\u0085(\3\2\2\2\u0086\u008a\t\n\2\2"+
		"\u0087\u008a\5\23\n\2\u0088\u008a\5\25\13\2\u0089\u0086\3\2\2\2\u0089"+
		"\u0087\3\2\2\2\u0089\u0088\3\2\2\2\u008a*\3\2\2\2\u008b\u008f\7a\2\2\u008c"+
		"\u008f\5\23\n\2\u008d\u008f\5\25\13\2\u008e\u008b\3\2\2\2\u008e\u008c"+
		"\3\2\2\2\u008e\u008d\3\2\2\2\u008f,\3\2\2\2\23\2EIMRU[^agjo}\u0081\u0084"+
		"\u0089\u008e\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}