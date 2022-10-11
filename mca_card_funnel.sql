-- Baseline params

SET START_DATE = '2020-01-01'; -- used as a reference point of profile creation
SET CARD_ISSUE_WINDOW =  30; -- all subsequent days must be within this value to the profile creation date
SET CARD_ACTIVATION_WINDOW =  30;
SET CARD_SPEND_5_TX_WINDOW =  30;

CREATE
OR REPLACE TABLE {{params.reports}}.mca_card_funnel AS(
select profile_id as profile_id,
       profile_type as profile_type,
       country_code as country_code,
       currency as currency,
       cohort_month as cohort_month,
       cohort_week as cohort_week,
       profile_date_created as profile_date_created,
       date as date,
       event as event,
       event_id as event_id
from (
          (
              select up.id                                                                                  profile_id,
                     IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                         'Business')                                                                        profile_type,
                     a.COUNTRY_CODE                                                                         country_code,
                     'N/A'                                                                                  currency,
                     date_trunc('month', up.DATE_CREATED)                                                   cohort_month,
                     date_trunc('week', up.DATE_CREATED)                                                    cohort_week,
                     up.date_created                                                                        profile_date_created,
                     up.DATE_CREATED                                                                        date,
                     '1. PROFILE_CREATED' as                                                                event,
                     up.id::string                                                                          event_id

              from PROFILE.USER_PROFILE up
                       join profile.ADDRESS a
                            ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS' -- what is the use of primary user profile address condition?
              where true
                and  profile_date_created >= $START_DATE
          )

     UNION ALL -- FILTER users who ordered a car within 30 days of profile creation
      (
          select profile_id,
                 profile_type,
                 country_code,
                 currency,
                 cohort_month,
                 cohort_week,
                 profile_date_created,
                 date,
                 '2. CARD ORDERED 30D' as event,
                 event_id::string
          from (
                   select up.id                                                                           profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                 profile_type,
                          a.COUNTRY_CODE                                                                  country_code,
                          date_trunc('month', up.DATE_CREATED)                                            cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                             cohort_week,
                          up.date_created                                                                 profile_date_created,
                          PCIS.FIRST_CARD_ORDER_TIME                                                      date,
                          CONCAT(PCIS.PROFILE_ID,PCIS.FIRST_CARD_ORDER_TIME)                            event_id,
                          'N/A'                                                                           currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            JOIN REPORTS.PROFILE_CARD_ISSUANCE_SUMMARY PCIS ON PCIS.USER_ID = a.USER_PROFILE_ID
                   where true
                     and  profile_date_created >= $START_DATE
               )
              where date is not null
            and date < DATEADD(day, $CARD_ISSUE_WINDOW, profile_date_created)
          )


     UNION ALL -- FILTER users who were issued a card within 30 days of profile creation
      (
          select profile_id,
                 profile_type,
                 country_code,
                 currency,
                 cohort_month,
                 cohort_week,
                 profile_date_created,
                 date,
                 '3. CARD ISSUED 30D' as event,
                 event_id::string
          from (
                   select up.id                                                                           profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                 profile_type,
                          a.COUNTRY_CODE                                                                  country_code,
                          date_trunc('month', up.DATE_CREATED)                                            cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                             cohort_week,
                          up.date_created                                                                 profile_date_created,
                          PCIS.FIRST_CARD_CREATION_TIME                                                   date,
                          CONCAT(PCIS.PROFILE_ID,PCIS.FIRST_CARD_CREATION_TIME)                         event_id,
                          'N/A'                                                                           currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            JOIN REPORTS.PROFILE_CARD_ISSUANCE_SUMMARY PCIS ON PCIS.USER_ID = a.USER_PROFILE_ID
                   where true
                     and  profile_date_created >= $START_DATE
               )
              where date is not null
            and date < DATEADD(day, $CARD_ISSUE_WINDOW, profile_date_created)
          )

     UNION ALL -- FILTER users who activated a card within 30 days of profile creation (i.e performed a tx)
      (
          select profile_id,
                 profile_type,
                 country_code,
                 currency,
                 cohort_month,
                 cohort_week,
                 profile_date_created,
                 date,
                 '4. CARD ACTIVATED 60D' as event,
                 event_id::string
          from (
                   select up.id                                                                           profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                 profile_type,
                          a.COUNTRY_CODE                                                                  country_code,
                          date_trunc('month', up.DATE_CREATED)                                            cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                             cohort_week,
                          up.date_created                                                                 profile_date_created,
                          PCIS.FIRST_ACTIVATION_TIME                                                      date,
                          CONCAT(PCIS.PROFILE_ID,PCIS.FIRST_ACTIVATION_TIME)                            event_id,
                          'N/A'                                                                           currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            JOIN REPORTS.PROFILE_CARD_ISSUANCE_SUMMARY PCIS ON PCIS.USER_ID = a.USER_PROFILE_ID
                   where true
                     and  profile_date_created >= $START_DATE
               )

                    where date is not null
            and date < DATEADD(day, $CARD_ACTIVATION_WINDOW, profile_date_created)
          )

     UNION ALL -- FILTER users who completed 5 tx card within 90 days of profile creation - The window period is increased since card is generally used while travelling. 
      (
          select profile_id,
                 profile_type,
                 country_code,
                 currency,
                 cohort_month,
                 cohort_week,
                 profile_date_created,
                 date,
                 '5. FIVE TX 90D' as event,
                 event_id::string
          from (
                   select up.id                                                                           profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                 profile_type,
                          a.COUNTRY_CODE                                                                  country_code,
                          date_trunc('month', up.DATE_CREATED)                                            cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                             cohort_week,
                          up.date_created                                                                 profile_date_created,
                          DATEADD(day,DAYS_FIRST_CARD_CREATION_TO_FIVE_TRANSACTION, profile_date_created) date,
                          CONCAT(PCIS.PROFILE_ID,PCIS.FIRST_ACTIVATION_TIME,DAYS_FIRST_CARD_CREATION_TO_FIVE_TRANSACTION) event_id,
                          'N/A'                                                                           currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            JOIN REPORTS.PROFILE_CARD_ISSUANCE_SUMMARY PCIS ON PCIS.USER_ID = a.USER_PROFILE_ID
                   where true
                     and  profile_date_created >= $START_DATE
               )
                where date is not null
            and date < DATEADD(day, $CARD_SPEND_5_TX_WINDOW, profile_date_created)
          )

     ));