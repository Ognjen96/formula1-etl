IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimTime')
    CREATE TABLE dbo.DimTime (
	    DateKey INT PRIMARY KEY,
	    DateValue DATE NOT NULL,
	    DayValue INT NOT NULL,
	    MonthValue INT NOT NULL,
	    YearValue INT NOT NULL
    );

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimRace')
    CREATE TABLE dbo.DimRace (
        RaceID INT PRIMARY KEY,
        RaceName VARCHAR(255) NOT NULL,
        RaceNumOfDrivers INT,
        RaceUrl VARCHAR(255),
        RaceFirstPractiseDate DATE,
        RaceSecondPractiseDate DATE,
        RaceThirdPractiseDate DATE,
        RaceQualiDate DATE,
        RaceSprintDate DATE,
        RaceRound INT
    );

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimDriver')
    CREATE TABLE dbo.DimDriver(
        DriverID INT PRIMARY KEY,
        DriverRef VARCHAR(255) NOT NULL,
        DriverName VARCHAR(255) NOT NULL,
        DriverNumber INT,
        DriverSurname VARCHAR(255) NOT NULL,
        DriverNationality VARCHAR(255),
        DriverCode VARCHAR(255),
        DateOfBirth DATE NOT NULL,
        DriverUrl VARCHAR(255)
    );

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimConstructor')
CREATE TABLE dbo.DimConstructor(
    ConstructorID INT PRIMARY KEY,
    ConstructorRef VARCHAR(255),
    ConstructorName VARCHAR(255),
    ConstructorNationality VARCHAR(255),
    ConstructorUrl VARCHAR(255)
        );

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimCircuit')
CREATE TABLE dbo.DimCircuit(
    CircuitID INT PRIMARY KEY,
    CircuitRef VARCHAR(255),
    CircuitName VARCHAR(255) NOT NULL,
    CircuitLocation VARCHAR(255),
    CircuitCountry VARCHAR(255),
    CircuitUrl VARCHAR(255),
    CircuitLat FLOAT,
    CircuitLng FLOAT,
    CircuitAlt FLOAT
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactResults')
    CREATE TABLE dbo.FactResults(
        ResultID INT PRIMARY KEY,
        ResultRaceID INT NOT NULL,
        ResultDriverID INT NOT NULL,
        ResultDateKey    INT,
        ResultConstructorID INT NOT NULL,
        ResultCircuitID INT NOT NULL,
        ResultPosition INT,
        ResultGrid INT,
        ResultPoints DECIMAL(4,1),
        ResultLaps INT,
        ResultTime VARCHAR(50),
        ResultMiliseconds INT,
        ResultFastestLap INT,
        ResultRank INT,
        ResultFastestLapTimeMs INT,
        ResultFastestLapSpeed FLOAT,
        ResultNumOfPitstops INT,
        ResultDurationPitstopsMs INT,
        ResultStatus VARCHAR(255),
        FOREIGN KEY (ResultRaceID) REFERENCES dbo.DimRace(RaceID),
        FOREIGN KEY (ResultDriverID) REFERENCES dbo.DimDriver(DriverID),
        FOREIGN KEY (ResultConstructorID) REFERENCES dbo.DimConstructor(ConstructorID),
        FOREIGN KEY (ResultCircuitID) REFERENCES dbo.DimCircuit(CircuitID),
        FOREIGN KEY(ResultDateKey) REFERENCES dbo.DimTime(DateKey)
    );