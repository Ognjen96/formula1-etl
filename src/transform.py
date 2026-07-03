import pandas as pd

#cols = ["resultId", "raceId", "driverId", "constructorId","circuitId", "position", "grid", "points", "laps", "time","milliseconds", "fastestLap", "rank", "fastestLapTime", "fastestLapSpeed", "status", "stop", "milliseconds_pitstops"]
dim_race_cols = ["raceId", "name_x", "number_drivers", "url_x", "fp1_date", "fp2_date", "fp3_date", "quali_date", "sprint_date", "round"]
dim_driver_cols = ["driverId", "driverRef", "forename", "number", "surname", "nationality", "code", "dob", "url"]
dim_constructor_cols = ["constructorId", "constructorRef", "name", "nationality_constructors", "url_constructors"]
dim_circuit_cols = ["circuitId", "circuitRef", "name_y", "location", "country", "url_y", "lat", "lng", "alt"]
dim_time_cols = ["dateKey", "date", "year", "month"]
fact_results_cols = ["resultId", "raceId", "driverId","date", "constructorId","circuitId", "position", "grid", "points", "laps", "time","milliseconds", "fastestLap", "rank", "fastestLapTime", "fastestLapSpeed", "status", "num_pitstops", "milliseconds_pitstops"]

def load_data(file_path):
    
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")
        return pd.DataFrame()
    
def clean_data(df):
    pitstop_information = df.drop_duplicates(subset=["resultId","stop"]).groupby("resultId").agg(num_pitstops=("stop", "count"), milliseconds_pitstops = ("milliseconds_pitstops", "sum"))
    general_info = df.drop(columns = ['stop', 'milliseconds_pitstops']).drop_duplicates(subset = ["resultId"])
    return general_info.merge(pitstop_information, on = "resultId", how = "left")

def dim_creation(cleaned_df):
    #DimConstructor
    dim_constructor = cleaned_df[dim_constructor_cols].drop_duplicates(subset = ["constructorId"])
    dim_constructor = dim_constructor.rename(columns = {
        "constructorId": "ConstructorID",
        "constructorRef": "ConstructorRef",
        "name": "ConstructorName",
        "nationality_constructors": "ConstructorNationality" ,
        "url_constructors": "ConstructorUrl"
    })
    dim_constructor.to_csv("/opt/airflow/data/dim_constructor.csv", index = False)
    
    #DimCircuit
    dim_circuit = cleaned_df[dim_circuit_cols].drop_duplicates(subset = ["circuitId"])
    dim_circuit = dim_circuit.rename(columns = {
        "circuitId": "CircuitID",
        "circuitRef": "CircuitRef",
        "name_y": "CircuitName",
        "location": "CircuitLocation",
        "country": "CircuitCountry",
        "url_y": "CircuitUrl",
        "lat": "CircuitLat",
        "lng": "CircuitLng",
        "alt": "CircuitAlt"
    })
    dim_circuit["CircuitAlt"] = pd.to_numeric(dim_circuit["CircuitAlt"], errors="coerce")
    dim_circuit.to_csv("/opt/airflow/data/dim_circuit.csv", index = False)

    #DimDriver
    dim_driver = cleaned_df[dim_driver_cols].drop_duplicates(subset = ["driverId"])
    dim_driver = dim_driver.rename(columns = {
        "driverId": "DriverID",
        "driverRef": "DriverRef", 
        "forename": "DriverName", 
        "number": "DriverNumber", 
        "surname": "DriverSurname", 
        "nationality": "DriverNationality", 
        "code": "DriverCode", 
        "dob": "DateOfBirth", 
        "url": "DriverUrl"
    })
    dim_driver.to_csv("/opt/airflow/data/dim_driver.csv", index = False)

    #DimRace 
    dim_race = cleaned_df[dim_race_cols].drop_duplicates(subset = ["raceId"])
    dim_race = dim_race.rename(columns = {
        "raceId": "RaceID",
        "name_x": "RaceName", 
        "number_drivers": "RaceNumOfDrivers",  
        "url_x": "RaceUrl", 
        "fp1_date": "RaceFirstPractiseDate", 
        "fp2_date": "RaceSecondPractiseDate", 
        "fp3_date": "RaceThirdPractiseDate", 
        "quali_date": "RaceQualiDate", 
        "sprint_date": "RaceSprintDate", 
        "round": "RaceRound"
    })
    date_cols = ["RaceFirstPractiseDate", "RaceSecondPractiseDate", "RaceThirdPractiseDate", "RaceQualiDate", "RaceSprintDate"]
    for date in date_cols:
        dim_race[date] = pd.to_datetime(dim_race[date], errors="coerce")
    dim_race.to_csv("/opt/airflow/data/dim_race.csv", index = False)

    #DimTime
    lowest_recorded_year = pd.to_datetime(cleaned_df["date"]).min().year
    highest_recorded_year = pd.to_datetime(cleaned_df["date"]).max().year

    end = f"{highest_recorded_year}-12-31"
    start = f"{lowest_recorded_year}-01-01"

    all_days = pd.date_range(start = start, end = end, freq="D")
    
    dim_time = pd.DataFrame(
        {"DateKey" : all_days.strftime("%Y%m%d").astype(int),
         "DateValue" : all_days,
         "YearValue" : all_days.year,
         "MonthValue" : all_days.month,
         "DayValue" : all_days.day}
    )
    dim_time.to_csv("/opt/airflow/data/dim_time.csv", index=False)

    return {"DimTime": "/opt/airflow/data/dim_time.csv", "DimRace": "/opt/airflow/data/dim_race.csv", "DimCircuit": "/opt/airflow/data/dim_circuit.csv", "DimConstructor": "/opt/airflow/data/dim_constructor.csv", "DimDriver": "/opt/airflow/data/dim_driver.csv"}

def fact_creation(cleaned_data):
    #FactResults
    fact_results = cleaned_data[fact_results_cols].drop_duplicates(subset = ["resultId"])
    fact_results["ResultDateKey"] = pd.to_datetime(fact_results["date"]).dt.strftime("%Y%m%d").astype(int)
    fact_results.drop(columns = ["date"], inplace = True)
    fact_results = fact_results.rename(columns = {
        "resultId": "ResultID",
        "raceId": "ResultRaceID", 
        "driverId": "ResultDriverID", 
        "constructorId": "ResultConstructorID",
        "circuitId": "ResultCircuitID", 
        "position": "ResultPosition", 
        "grid": "ResultGrid", 
        "points": "ResultPoints", 
        "laps": "ResultLaps", 
        "time": "ResultTime",
        "milliseconds": "ResultMiliseconds", 
        "fastestLap": "ResultFastestLap", 
        "rank": "ResultRank", 
        "fastestLapTime": "ResultFastestLapTimeMs", 
        "fastestLapSpeed": "ResultFastestLapSpeed", 
        "status": "ResultStatus",
        "num_pitstops": "ResultNumOfPitstops", 
        "milliseconds_pitstops": "ResultDurationPitstopsMs"
    })
    
    fact_results["ResultMiliseconds"] = pd.to_numeric(fact_results["ResultMiliseconds"], errors = "coerce")
    fact_results["ResultFastestLap"] = pd.to_numeric(fact_results["ResultFastestLap"], errors = "coerce")
    fact_results["ResultFastestLapSpeed"] = pd.to_numeric(fact_results["ResultFastestLapSpeed"], errors = "coerce")
    fact_results["ResultPosition"] = pd.to_numeric(fact_results["ResultPosition"], errors = "coerce")
    fact_results.to_csv("/opt/airflow/data/fact_results.csv", index = False)
    print(fact_results.dtypes)


    return {"FactResults" : "/opt/airflow/data/fact_results.csv"}