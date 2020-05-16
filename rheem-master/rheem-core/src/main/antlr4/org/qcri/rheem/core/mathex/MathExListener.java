// Generated from /Users/edward/Code/Lab/rheem_arrow/rheem-master/rheem-core/src/main/antlr4/org/qcri/rheem/core/mathex/MathEx.g4 by ANTLR 4.8
package org.qcri.rheem.core.mathex;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link MathExParser}.
 */
public interface MathExListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by the {@code constant}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterConstant(MathExParser.ConstantContext ctx);
	/**
	 * Exit a parse tree produced by the {@code constant}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitConstant(MathExParser.ConstantContext ctx);
	/**
	 * Enter a parse tree produced by the {@code function}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterFunction(MathExParser.FunctionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code function}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitFunction(MathExParser.FunctionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code variable}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterVariable(MathExParser.VariableContext ctx);
	/**
	 * Exit a parse tree produced by the {@code variable}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitVariable(MathExParser.VariableContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parensExpression}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterParensExpression(MathExParser.ParensExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parensExpression}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitParensExpression(MathExParser.ParensExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code binaryOperation}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterBinaryOperation(MathExParser.BinaryOperationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code binaryOperation}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitBinaryOperation(MathExParser.BinaryOperationContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unaryOperation}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterUnaryOperation(MathExParser.UnaryOperationContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unaryOperation}
	 * labeled alternative in {@link MathExParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitUnaryOperation(MathExParser.UnaryOperationContext ctx);
}