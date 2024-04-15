-- Regiões
CREATE TABLE dbo.TB_Regioes
(
	RegId		int	            NOT NULL,
	Nome		varchar(100)	NOT NULL,

	CONSTRAINT PK_Regioes PRIMARY KEY CLUSTERED (RegId)
);
GO

CREATE INDEX IX_Regioes_Nome ON dbo.TB_Regioes(Nome);
GO

-- Temperaturas
CREATE TABLE dbo.TB_Temperaturas
(
	RegId	    int             NOT NULL,
	Data		date	        NOT NULL,
    TempMin     decimal(10,8)   NOT NULL,
    TempMax     decimal(10,8)   NOT NULL,
    TempMed     decimal(10,8)   NOT NULL,

	CONSTRAINT PK_Temperaturas PRIMARY KEY CLUSTERED (RegId, Data),
	CONSTRAINT FK_Temperaturas_Regioes FOREIGN KEY (RegId) REFERENCES TB_Regioes (RegId) ON DELETE NO ACTION
);
GO

-- Predições
CREATE TABLE dbo.TB_Predicoes
(
	RegId	    int             NOT NULL,
	Data		date	        NOT NULL,
    TempMed     decimal(10,8)   NOT NULL,

	CONSTRAINT PK_Predicoes PRIMARY KEY CLUSTERED (RegId, Data),
	CONSTRAINT FK_Predicoes_Regioes FOREIGN KEY (RegId) REFERENCES TB_Regioes (RegId) ON DELETE NO ACTION
);
GO


-- Stored Procedures
CREATE PROCEDURE usp_Temperaturas_Upsert 
	@prmRegId		int,
	@prmData		date,
	@prmTempMin		decimal(10,8),
	@prmTempMax		decimal(10,8),
	@prmTempMed		decimal(10,8)
AS
BEGIN

	MERGE INTO
		dbo.TB_Temperaturas AS tgt
	USING
		(VALUES (@prmRegId, @prmData, @prmTempMin, @prmTempMax, @prmTempMed)) AS src (RegId, Data, TempMin, TempMax, TempMed)
	ON
		tgt.RegId = src.RegId AND tgt.Data = src.Data
	WHEN MATCHED THEN
		UPDATE SET
			TempMin = @prmTempMin,
			TempMax = @prmTempMax,
			TempMed = @prmTempMed
	WHEN NOT MATCHED THEN
		INSERT
			(RegId, Data, TempMin, TempMax, TempMed)
		VALUES
			(src.RegId, src.Data, src.TempMin, src.TempMax, src.TempMed);

END
GO

CREATE PROCEDURE usp_Temperaturas_SelectForPrediction
	@prmRegId		int
AS
BEGIN

	SELECT
		Data,
		TempMed
	FROM
		TB_Temperaturas
	WHERE
		RegId = @prmRegId
	ORDER BY
		Data;

END
GO

CREATE PROCEDURE usp_Predicoes_Clean
	@prmRegId		int
AS
BEGIN

	DELETE
	FROM
		TB_Predicoes
	WHERE
		RegId = @prmRegId

END
GO

CREATE PROCEDURE usp_Predicoes_Insert
	@prmRegId		int,
	@prmData		date,
	@prmTempMed		decimal(10,8)
AS
BEGIN

	INSERT INTO
		TB_Predicoes
		(
			RegId,
			Data,
			TempMed
		)
	VALUES
		(
			@prmRegId,
			@prmData,
			@prmTempMed
		)

END
GO
