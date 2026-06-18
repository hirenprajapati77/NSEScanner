import io
import warnings
import pandas as pd

def test_pandas_read_json_deprecation_fix():
    # Create a simple DataFrame with a DatetimeIndex
    df = pd.DataFrame({'value': [1, 2, 3]}, index=pd.date_range('2023-01-01', periods=3))
    
    # Serialize it exactly as scanner.py does
    json_str = df.to_json(date_format='iso', orient='split')
    
    # Read it back using io.StringIO and ensure no FutureWarning is raised
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        
        # Read the json via string IO
        df_restored = pd.read_json(io.StringIO(json_str), orient='split')
        
        # Check if FutureWarning or other warnings were raised about literal json
        warning_msgs = [str(warning.message) for warning in w if "literal json" in str(warning.message).lower()]
        assert len(warning_msgs) == 0, f"Expected no 'literal json' warning, but got: {warning_msgs}"
        
    # Check that the index is a DatetimeIndex
    assert isinstance(df_restored.index, pd.DatetimeIndex), f"Expected DatetimeIndex, got {type(df_restored.index)}"

if __name__ == "__main__":
    test_pandas_read_json_deprecation_fix()
    print("Test passed successfully.")
