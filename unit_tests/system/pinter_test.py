import pytest
import pandas as pd
from system.utils.printer_utils import Color
from system.printer import Now, DataFramePrint, ProgressBarPrint


def test_now_prefix_formatting():
    n = Now("proc", "script")
    prefix = n._prefix()
    assert "PROC" in prefix
    assert "SCRIPT" in prefix
    assert str(Color.BLUE) in prefix

def test_now_format_info():
    n = Now("proc", "script")
    msg = n._format(Color.BLUE, "hello", "world")
    assert "hello world" in msg
    assert str(Color.BLUE) in msg


def test_print_current_line_return():
    n = Now("p", "s")
    result = n.print_current_line(do_print=False)
    assert result.startswith("line:")


def test_print_caller_return(monkeypatch):
    # Mock version < 3.11
    monkeypatch.setattr("sys.version_info", (3, 10))
    n = Now("x", "y")

    def fake_stack():
        class F:
            function = "test_func"
            filename = "file.py"
        return [None, F()]

    monkeypatch.setattr("inspect.stack", fake_stack)
    func, file = n.print_caller(do_print=False)
    assert func == "test_func"
    assert file == "file.py"


def test_dataframe_formatting():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    out = DataFramePrint.format(df)
    assert " a " in out
    assert " b " in out


def test_progress_bar_compute():
    p = ProgressBarPrint(bar_width=10, title="TEST")
    bar = p._compute_bar(5, 10)
    assert "TEST:" in bar
    assert "%" in bar
    assert "\x1b" in bar  # ANSI code present


if __name__ == "__main__":
    pytest.main([__file__])