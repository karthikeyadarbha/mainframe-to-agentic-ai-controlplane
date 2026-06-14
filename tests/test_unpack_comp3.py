import sys
import pathlib
import types
import pytest

# Ensure repository root is on PYTHONPATH so `src` can be imported
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Inject lightweight pyspark mocks so engine module can be imported in CI/test
mock_pyspark = types.ModuleType("pyspark")
mock_sql = types.ModuleType("pyspark.sql")
mock_functions = types.ModuleType("pyspark.sql.functions")
mock_types = types.ModuleType("pyspark.sql.types")

class MockColumn:
    def __init__(self, expr):
        self.expr = expr
    def __eq__(self, other):
        other_expr = other.expr if hasattr(other, "expr") else repr(other)
        return MockColumn(f"({self.expr} == {other_expr})")
    def __repr__(self):
        return f"MockColumn({self.expr})"
    def cast(self, t):
        return MockColumn(f"cast({self.expr} as {t})")
    def __mul__(self, other):
        other_expr = other.expr if hasattr(other, "expr") else other
        return MockColumn(f"({self.expr} * {other_expr})")
    def __rmul__(self, other):
        return MockColumn(f"({other} * {self.expr})")
    def __add__(self, other):
        other_expr = other.expr if hasattr(other, "expr") else other
        return MockColumn(f"({self.expr} + {other_expr})")
    def __radd__(self, other):
        return MockColumn(f"({other} + {self.expr})")
    def __truediv__(self, other):
        other_expr = other.expr if hasattr(other, "expr") else other
        return MockColumn(f"({self.expr} / {other_expr})")
    def __rtruediv__(self, other):
        return MockColumn(f"({other} / {self.expr})")

def _expr(s):
    return MockColumn(s)

# minimal functions used during import and expression construction
mock_functions.expr = _expr
mock_functions.hex = lambda c: MockColumn(f"hex({c})")
mock_functions.substring = lambda c, p, l: MockColumn(f"substring({c},{p},{l})")
mock_functions.length = lambda c: MockColumn(f"length({c})")
mock_functions.conv = lambda s, a, b: MockColumn(f"conv({s},{a},{b})")
mock_functions.lit = lambda v: MockColumn(repr(v))
class MockWhenResult:
    def __init__(self, cond, val):
        self.cond = cond
        self.val = val
    def otherwise(self, other):
        other_expr = other.expr if hasattr(other, "expr") else repr(other)
        return MockColumn(f"when({self.cond.expr},{self.val.expr}).otherwise({other_expr})")

mock_functions.when = lambda cond, val: MockWhenResult(cond, val)
mock_functions.col = lambda name: MockColumn(name)

mock_types.DoubleType = float

sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.sql"] = mock_sql
sys.modules["pyspark.sql.functions"] = mock_functions
sys.modules["pyspark.sql.types"] = mock_types

# Minimal SparkSession mock to satisfy `from pyspark.sql import SparkSession`
class MockSparkSession:
    class builder:
        @staticmethod
        def appName(name):
            return MockSparkSession.builder

        @staticmethod
        def master(m):
            return MockSparkSession.builder

        @staticmethod
        def config(k, v):
            return MockSparkSession.builder

        @staticmethod
        def getOrCreate():
            return MockSparkSession()

    def sparkContext(self):
        class SC:
            def setLogLevel(self, lvl):
                pass
        return SC()

# expose attributes on the sql module
mock_sql.SparkSession = MockSparkSession
mock_sql.functions = mock_functions
mock_sql.types = mock_types

from src.engine import EnterpriseMedallionDataVaultEngine


def test_unpack_comp3_expression_builds():
    # Call the method unbound to avoid initializing a real SparkSession
    fn = EnterpriseMedallionDataVaultEngine.unpack_comp3_udf
    # should not raise and should return a MockColumn-like object when invoked with a column name
    col = fn(None, "raw_balance")
    assert hasattr(col, "expr")
