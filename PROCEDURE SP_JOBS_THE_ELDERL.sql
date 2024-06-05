CREATE PROCEDURE SP_JOBS_THE_ELDERLY(
  P_PARAM1 IN VARCHAR2 -- 'D'를 받을 수 있는 파라미터
) AS
  -- 로그 저장을 위한 변수
  V_START_TIME    DATE;
  V_END_TIME      DATE;
  V_LOG_MESSAGE   VARCHAR2(4000);
  V_STEP          NUMBER := 0; -- 단계 변수 초기화

  -- 에러 처리를 위한 변수
  V_ERROR_MESSAGE VARCHAR2(4000);
  V_CURRENT_YEAR  NUMBER;


BEGIN
  -- 현재 연도 설정
  SELECT EXTRACT(YEAR FROM SYSDATE) INTO V_CURRENT_YEAR FROM DUAL;

  -- 단계 1: 시작 로그 저장
  V_STEP := 1;
  V_START_TIME := SYSDATE;
  V_LOG_MESSAGE := 'Procedure started';
  INSERT
  INTO
    TB_PROCEDURE_LOG
  (
    LOG_TIME, STEP, MESSAGE, STATUS
  )
  VALUES
    (
      V_START_TIME, V_STEP, V_LOG_MESSAGE, 'start'
    );
  COMMIT;

  -- 파라미터가 'D'인 경우 실행 기준 연도 데이터를 삭제
  IF P_PARAM1 = 'D' THEN
    V_STEP := 2;
    V_LOG_MESSAGE := 'Deleting data for the current year';
    INSERT
    INTO
      TB_PROCEDURE_LOG
    (
      LOG_TIME, STEP, MESSAGE, STATUS
    )
    VALUES
      (
          SYSDATE, V_STEP, V_LOG_MESSAGE, 'in_progress'
      );
    COMMIT;

    BEGIN
      DELETE FROM TB_DEVELOPER_DATA_GROUPED WHERE YEAR = V_CURRENT_YEAR;
      COMMIT;
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK;
        V_ERROR_MESSAGE := 'Error occurred while deleting data: ' || SQLERRM;
        INSERT
        INTO
          TB_PROCEDURE_ERROR_LOG
        (
          ERROR_TIME, STEP, MESSAGE
        )
        VALUES
          (
              SYSDATE, V_STEP, V_ERROR_MESSAGE
          );
        COMMIT;
        RAISE;
    END;
  END IF;

  -- 단계 3: 메인 스크립트 실행
  BEGIN
    V_STEP := 3;
    V_LOG_MESSAGE := 'Executing main script';
    INSERT
    INTO
      TB_PROCEDURE_LOG
    (
      LOG_TIME, STEP, MESSAGE, STATUS
    )
    VALUES
      (
          SYSDATE, V_STEP, V_LOG_MESSAGE, 'in_progress'
      );
    COMMIT;

    /* 1. 데이터 전처리 */
    INSERT
    INTO
      TB_DEVELOPER_DATA_GROUPED
    SELECT
      YEAR
    , SUBSTR(CITY_CODE, 0, 2) || '00000000'
    , CITY_NAME
    , SUM(TARGET_JOB)
    FROM
      TB_DEVELOPER_DATA
    WHERE
        1 = 1
    AND APPROVAL_STATUS IN ('승인완료', '임시')
    AND DELETION_STATUS = 'N'
    GROUP BY
      YEAR
    , CITY_NAME
    , SUBSTR(CITY_CODE, 0, 2) || '00000000'
    ;
    
    COMMIT;

    /* 2. 데이터마트 생성 */
    INSERT
    INTO
      TB_JOBS_FOR_SENIOR
    SELECT
      A.YEAR
    , A.CITY_CODE
    , CITY
    , TOTAL
    , OVER_60_64
    , OVER_65_69
    , OVER_70_74
    , OVER_75_79
    , OVER_80_84
    , OVER_85
    , MALE
    , FEMALE
    , B.TARGET_JOB
    FROM
      TB_KOSIS_DATA A
    , TB_DEVELOPER_DATA_GROUPED B
    WHERE
        1 = 1
    AND A.CITY_CODE = B.CITY_CODE_L
    AND A.YEAR = B.YEAR
    AND A.YEAR = '2022'
    AND B.YEAR = '2022'
    ;
    
    COMMIT;

    -- 작업이 성공하면 성공 로그 저장
    V_END_TIME := SYSDATE;
    V_LOG_MESSAGE := 'Procedure completed successfully';
    INSERT
    INTO
      TB_PROCEDURE_LOG
    (
      LOG_TIME, STEP, MESSAGE, STATUS
    )
    VALUES
      (
        V_END_TIME, V_STEP, V_LOG_MESSAGE, 'end'
      );
    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      -- 에러 발생 시 롤백
      ROLLBACK;
      -- 에러 로그 저장
      V_END_TIME := SYSDATE;
      V_ERROR_MESSAGE := 'Error occurred: ' || SQLERRM;
      INSERT
      INTO
        TB_PROCEDURE_ERROR_LOG
      (
        ERROR_TIME, STEP, MESSAGE
      )
      VALUES
        (
          V_END_TIME, V_STEP, V_ERROR_MESSAGE
        );
      COMMIT;
      -- 에러를 다시 발생시켜 호출자에게 알림
      RAISE;
  END;

  -- 단계 4: 프로시저 종료
  V_STEP := 4;
  V_END_TIME := SYSDATE;
  V_LOG_MESSAGE := 'Procedure ended';
  INSERT
  INTO
    TB_PROCEDURE_LOG
  (
    LOG_TIME, STEP, MESSAGE, STATUS
  )
  VALUES
    (
      V_END_TIME, V_STEP, V_LOG_MESSAGE, 'end'
    );
  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    -- 에러 발생 시 롤백
    ROLLBACK;
    -- 에러 로그 저장
    V_END_TIME := SYSDATE;
    V_ERROR_MESSAGE := 'Unexpected error occurred: ' || SQLERRM;
    INSERT
    INTO
      TB_PROCEDURE_ERROR_LOG
    (
      ERROR_TIME, STEP, MESSAGE
    )
    VALUES
      (
        V_END_TIME, V_STEP, V_ERROR_MESSAGE
      );
    COMMIT;
    -- 에러를 다시 발생시켜 호출자에게 알림
    RAISE;

END SP_JOBS_THE_ELDERLY;
/

