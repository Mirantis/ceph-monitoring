# Enter your code here. Read input from STDIN. Print output to STDOUT
from enum import Enum
import collections
import string
import sys
from typing import Dict, List, Any, Tuple, Iterator, Union, Optional, NamedTuple


# Tokenizer and parser code
# It's silly to write them manually - in normal project I would use something line PLY

class Token(Enum):
    """Token types for tokenizer output"""
    open_br = 0
    close_br = 1
    space = 2
    number = 3
    var = 4
    minus = 5
    plus = 6

    # faked token for control flow. Mean beginning of the (sub)expression
    beginning = 7

    # faked token for control flow, mean that where just have valid token before this
    expr = 8


class AccumulatorState(Enum):
    """Enum for tokenizer chars accumulator, which allows to aggregate spaces and digits into single token"""
    no_state = 0
    spaces = 1
    number = 2


# type for token body
TokenVal = Union[str, int, None]


class Term(NamedTuple):
    # actually it is Union[None, str, Expression], but python don't support recursive types yet
    var: Union[None, str, Any]
    coef: int


Expression = List[Term]

# maps variable name to it coefficient after simplification
# None is used as name for absolute term
SimpleExpression = Dict[Optional[str], int]


# it's generally not recommended to use custom exception
# but in this case I'd like to separate possible errors in my parser, which might produce ValueError
# from error which parser provides explicitly to mark unexpected input
class ParseError(ValueError):
    """General parsing error"""
    def __init__(self, offset: int, message: str, expr: str = None) -> None:
        self.offset = offset
        self.message = message
        self.expr = expr

    def __str__(self) -> str:
        if self.expr and self.offset >= 0:
            expr = f" in '{self.expr[:self.offset]} >>> {self.expr[self.offset:]}'"
        else:
            expr = ""
        return f"Parse error: {self.message} before offset {self.offset}{expr}"


class UnexpectedTokenError(ParseError):
    """Unexpected token"""
    def __init__(self, offset: int, expected: str, got: Any) -> None:
        ParseError.__init__(self, offset, f"Unexpected token: {got}. Expected {expected}")


def tokenize(data: str) -> Iterator[Tuple[int, Token, TokenVal]]:
    """
    Transform input string into stream of
    (offset, token_type, token_body)
    """
    # used to accumulate spaces and digits
    acc_state = AccumulatorState.no_state
    accumulate: Optional[str] = None

    # current offset in the string, -1 used as a marker for empty string
    offset = -1

    for offset, char in enumerate(data):
        if acc_state == AccumulatorState.spaces:
            # assertions are used to check logic error only, not for user input errors
            assert accumulate
            if char == ' ':
                accumulate += " "
                continue
            yield offset, Token.space, accumulate
            accumulate = None
            acc_state = AccumulatorState.no_state
            # continue to process current char below
        elif acc_state == AccumulatorState.number:
            assert accumulate
            if char.isdigit():
                accumulate += char
                continue
            yield offset, Token.number, int(accumulate)
            accumulate = None
            acc_state = AccumulatorState.no_state
            # continue to process current char below

        assert acc_state == AccumulatorState.no_state

        if char == '-':
            yield offset, Token.minus, None
        elif char == '+':
            yield offset, Token.plus, None
        elif char == '(':
            yield offset, Token.open_br, None
        elif char == ')':
            yield offset, Token.close_br, None
        elif char == ' ':
            assert accumulate is None
            accumulate = " "
            acc_state = AccumulatorState.spaces
        elif char.isdigit():
            assert accumulate is None
            accumulate = char
            acc_state = AccumulatorState.number
        elif char in string.ascii_lowercase:
            yield offset, Token.var, char
        else:
            raise UnexpectedTokenError(offset, " +-()[0-9][a-z]", char)

    # process last number, if expression ends with number
    # we should not care about trailing spaces, so just drop them
    # in more general tokenizer trailing spaces better to be passed to upper level as well
    if acc_state == AccumulatorState.number:
        assert accumulate
        assert offset >= 0
        yield offset, Token.number, int(accumulate)


def do_parse(tokens: Iterator[Tuple[int, Token, TokenVal]], subexpr: bool) -> Expression:

    # prev_token always belong to {beginning, expr, number, minus, plus}
    # can not be in {open_br, close_br, space, var}
    prev_token = Token.beginning

    # coefficient for next variable
    coef: Optional[int] = None

    # current result. It's possible to yield Terms to upper level immediately
    # without aggregating them in list, but this would makes some checks harder
    # also hard to yield from for subexpressions
    expression: Expression = []
    offset = -1

    # be aware the this iterator get reused and partially consumed in recursive do_parse calls
    # this allows get rid of double scan for closing bracket and get O(n) parse time
    for offset, token_tp, val in tokens:
        if prev_token == Token.beginning:
            # first token can be {number, open bracket, space, or close bracket}
            if token_tp == Token.number:
                assert coef is None
                assert isinstance(val, int) and val >= 0
                coef = val
                prev_token = Token.number
            elif token_tp == Token.open_br:
                expression.append(Term(do_parse(tokens, subexpr=True), 1))
                prev_token = Token.expr
            elif token_tp == Token.space:
                pass
            elif token_tp in (Token.minus, Token.plus):
                # should '+' be disallowed here as well? it's not explicitly disabled, yet make no sense
                raise ParseError(offset, "+/- can not be the first non-space symbol")
            else:
                raise UnexpectedTokenError(offset,
                                           "space, number or '('" if not subexpr else "space, number or '('", val)
        elif prev_token == Token.expr:
            # first token can be {number, open bracket, space, or close bracket}
            if token_tp == Token.space:
                pass
            elif token_tp in (Token.minus, Token.plus):
                prev_token = token_tp
            elif token_tp == Token.close_br and subexpr:
                # subexpression finished
                break
            else:
                raise UnexpectedTokenError(offset, "space, -+, or ')'", val)
        elif prev_token == Token.number:
            # first token can be {var, sign, space or close brackets}
            if token_tp == Token.var:
                assert coef is not None
                expression.append(Term(val, coef))
                prev_token = Token.expr
            elif token_tp in (Token.minus, Token.plus, Token.space):
                assert coef is not None
                expression.append(Term(None, coef))
                prev_token = Token.expr if token_tp == Token.space else token_tp
            elif token_tp == Token.close_br and subexpr:
                # subexpression finished
                break
            else:
                raise UnexpectedTokenError(offset, "space, var or sign", val)
            coef = None
        elif prev_token in (Token.minus, Token.plus):
            # cah only be {number, open bracket, space}
            if token_tp == Token.number:
                assert coef is None
                assert isinstance(val, int) and val >= 0
                coef = val if prev_token == Token.plus else -val
                prev_token = Token.number
            elif token_tp == Token.open_br:
                sub_coef = -1 if prev_token == Token.minus else 1
                expression.append(Term(do_parse(tokens, subexpr=True), sub_coef))
                prev_token = Token.expr
            elif token_tp == Token.space:
                pass
            else:
                raise UnexpectedTokenError(offset, "open bracket or space", val)
        else:
            assert False, f"Unexpected prev token {prev_token}"

    # valid expression can not ends on anything except number or expr
    if prev_token not in (Token.number, Token.expr):
        raise ParseError(offset, f"Incorrect last token, should be number or expression, got {prev_token}")

    # there can be last number in buffer, appending it to result
    if coef is not None or prev_token == Token.number:
        assert coef is not None and prev_token == Token.number, f"{prev_token}, {coef}"
        expression.append(Term(None, coef))

    # expression should have at least two tokens
    if len(expression) < 2:
        raise ParseError(offset, "too few terms")

    return expression


def simplify(expr: Expression) -> SimpleExpression:
    """recursively unfolding expressions and adding up all coefficients"""
    result: SimpleExpression = collections.Counter()
    for var, vcoef in expr:
        if isinstance(var, list):
            for name, coef in simplify(var).items():
                result[name] += coef * vcoef
        else:
            result[var] += vcoef
    return {k: v for k, v in result.items() if v != 0}


def format_simple(expr: SimpleExpression) -> str:
    """formatting result"""
    res = []
    for name, val in sorted(expr.items(), key=lambda x: chr(ord('z') + 1) if x[0] is None else x[0]):
        if name is None:
            name = ""
        if val < 0:
            res.extend(["-", f"{-val}{name}"])
        else:
            res.extend(["+", f"{val}{name}"])

    if not res:
        return ""

    if res[0] == '+':
        res = res[1:]

    return " ".join(res)


def parse(data: str) -> Expression:
    """helper function to hide tokenizer and extra options of do_parse"""
    try:
        return do_parse(tokenize(data), subexpr=False)
    except ParseError as exc:
        exc.expr = data
        raise


def tests():
    incorrect_expressions = {
        # single or zero expressions not allowed
        "1", "x", "", "1x", "-1x", "- 2x", "7",

        # each variable should have coefficient, and no space allowed between coefficient and variable
        "1y+2 x", "1y + x", "1y+x",

        # minus should not be the first symbol in the expression
        "-1x+4y", "-7+5y", "-3", "3+(-1x+4)",

        # variable name should be one small english letter
        "4x+2ty", "4C+6t",

        "1x+3y+", "1x+3y-", "1x + 3 -", "1x +(4+5)+",

        # incorrect syntax
        "1 2 3", "7 +- 1x", "1y -+ 1x", "3y +- 1x", "4y ++ 2x", "4y -- 2x", "1x+", "+-+", "1x=2y", "(1x+2y) 7",
        "(1x+2y) 1x", "1x 5"

        # each expression and subexpression about have at least two terms
        "1x+2y+()", "14x+(2y)", "((14x-z))", "(13)",

        # first plug sign makes no sense (thought not explicitly disabled)
        "+3x+y", "3 - (+4x + 6)"
    }

    for expr in incorrect_expressions:
        try:
            parse(expr)
        except ParseError:
            pass
        except Expression as exc:
            raise AssertionError(f"Incorrect error {exc} during parsing {expr!r}")
        else:
            raise AssertionError(f"Failed to find error during parsing {expr!r}")

    expressions = {
        "1x+2y": "1x + 2y",
        "1x+ 2y +13x -2x -7": "12x + 2y - 7",
        "0x+ 2y": "2y",
        "2x + 5": "2x + 5",
        "2x+5": "2x + 5",
        "5 + 2x": "2x + 5",
        "2x+ 2y -2x": "2y",
        "2y+ 2x": "2x + 2y",
        "222y+ 211x": "211x + 222y",
        "2x+ 2y -1x - 1x": "2y",
        "7-7": "",
        "1+2+3 + 4": "10",
        "1+2+3 + 4     ": "10",
        "     1+2+3 + 4     ": "10",
        "1 + (1x + 2y)": "1x + 2y + 1",
        "4r + (1x - (2y + 1t + 2r - 1x))": "2r - 1t + 2x - 2y",
        "4r + (1x - (2y + 1t + 2r + 1x))": "2r - 1t - 2y",
        "4r - (1x - (2y + 1t + 2r + 1x))": "6r + 1t + 2y",
        "((4r - 1x) - (2y + 1t)) - 2r - 1x": "2r - 1t - 2x - 2y",
        "((4r - 1x) - (2y + 1t)) - 4r + 1x": "- 1t - 2y",
        "1x+(1x+1)": "2x + 1",
        "1x+(1x+1)     ": "2x + 1",
        "(1x+ 2y) + (3x + 2t) + 7": "2t + 4x + 2y + 7",
        "(1x+ 2y - (3x + 2t)) + 7": "- 2t - 2x + 2y + 7",
        "(1x+ 2y - (3x + 2t)) + 7c": "7c - 2t - 2x + 2y",
        "+".join("1x" for i in range(1000)): "1000x",
        "1x+" + "".join("(1x +" for i in range(100)) + "1" + ")" * 100: "101x + 1",

        # expressions from the task
        "2x + 6y - (12 + (5x - 3y)) + 4": "- 3x + 9y - 8",
        "2x + ( 6y + 12  )": "2x + 6y + 12",
        "2x+(6y+12)": "2x + 6y + 12",
        "1+ 2" : "3",
        "(3d-2)+6": "3d + 4",
        "1b-1c+(1+1c+1a)": "1a + 1b + 1",
        "2x - (4 + 4x + (18 - 2x))": "- 22",
    }

    for input_expr, expected_output in expressions.items():
        output = format_simple(simplify(parse(input_expr)))
        assert output == expected_output, f"{output!r} != {expected_output!r}"


def main(argv: List[str]) -> int:
    num_lines = int(sys.stdin.readline())
    for _ in range(num_lines):
        expr = sys.stdin.readline().strip()
        print(format_simple(simplify(parse(expr))))
    return 0


if __name__ == "__main__":
    # tests()
    exit(main(sys.argv))
