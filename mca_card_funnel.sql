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
                and profile_date_created >= '2017-01-01'
          )

     UNION ALL
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
                     and profile_date_created >= '2017-01-01'
               )
              where date is not null
            and date < DATEADD(day, 30, profile_date_created)
          )


     UNION ALL
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
                     and profile_date_created >= '2017-01-01'
               )
              where date is not null
            and date < DATEADD(day, 30, profile_date_created)
          )

     UNION ALL
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
                     and profile_date_created >= '2017-01-01'
               )

                    where date is not null
            and date < DATEADD(day, 60, profile_date_created)
          )

     UNION ALL
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
                     and profile_date_created >= '2017-01-01'
               )
                where date is not null
            and date < DATEADD(day, 90, profile_date_created)
          )

     ));