CREATE
OR REPLACE TABLE {{params.reports}}.mca_balance_funnel AS(
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
                 '2. BALANCE_OPENED' as event,
                 event_id::string
          from (
                   select up.id                                profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                      profile_type,
                          a.COUNTRY_CODE                       country_code,
                          date_trunc('month', up.DATE_CREATED) cohort_month,
                          date_trunc('week', up.DATE_CREATED)  cohort_week,
                          mcab.CURRENCY                        currency, -- should only select a single balance - For on boarding funnel - only interested in whether a balance was opened or not (boolean)
                          mcab.CREATION_TIME                   date,
                          up.DATE_CREATED                      profile_date_created,
                          mcab.id                              event_id
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join balance.ACCOUNT mca ON mca.profile_id = up.id
                            join BALANCE.BALANCE mcab on mca.ID = mcab.ACCOUNT_ID
                   where true -- placeholder text - no functional impact (ignore)
                     and profile_date_created >= '2017-01-01'
               )
          where date is not null
            and date < DATEADD(day, 30, profile_date_created) -- should be mentioned in the description (unless SOP) + what's the objective and how is this 30 day value computed?
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
                 '3. TOPPED UP' as event,
                 event_id::string
          from (
                   select up.id                                                                            profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                  profile_type,
                          a.COUNTRY_CODE                                                                   country_code,
                          date_trunc('month', up.DATE_CREATED)                                             cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                              cohort_week,
                          up.date_created                                                                  profile_date_created,
                          BTX.CREATION_TIME                                                                date,
                          BTX.ID                                                                           event_id,
                          MCAB.CURRENCY                                                                    currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join balance.ACCOUNT mca ON mca.profile_id = up.id
                            join BALANCE.BALANCE mcab on mca.ID = mcab.ACCOUNT_ID
                            join BALANCE.TRANSACTION BTX ON BTX.ACCOUNT_ID = mca.ID
                   where true
                     and profile_date_created >= '2017-01-01'
                     and BTX.STATE = 'COMPLETED'
                     AND BTX.TYPE = 'DEPOSIT'

                   qualify row_number() over (partition by up.id order by BTX.CREATION_TIME) = 1
               )
          where date is not null
            and date < DATEADD(day, 30, profile_date_created)
      )
      UNION ALL -- VERIFICATION : NEED TO FIND TABLE - skip for now
      (
          select profile_id,
                 profile_type,
                 country_code,
                 currency,
                 cohort_month,
                 cohort_week,
                 profile_date_created,
                 date,
                 '4. VERIFIED' as event,
                 event_id::string
          from (
                   select up.id                                                     profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                           profile_type,
                          a.COUNTRY_CODE                                            country_code,
                          date_trunc('month', up.DATE_CREATED)                      cohort_month,
                          date_trunc('week', up.DATE_CREATED)                       cohort_week,
                          up.date_created                                           profile_date_created,
                          coalesce(VERIFICATION_COMPLETION_TIME, BANKDETAIL_INITIATION_TIME) date,
                          bdos.order_id                                             event_id,
                          bdos.CURRENCY                                             currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join REPORTS.REPORT_FC_BANK_DETAILS_ORDER_STATUS bdos ON bdos.profile_id = up.id
                   where true
                     and profile_date_created >= '2017-01-01'
                     and (VERIFICATION_STATUS = 'DONE')
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
                 '4. CCY TX WITHIN 30 DAYS' as event,
                 event_id::string
          from (
                   select up.id                                                                                        profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                              profile_type,
                          a.COUNTRY_CODE                                                                               country_code,
                          date_trunc('month', up.DATE_CREATED)                                                         cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                                          cohort_week,
                          up.date_created                                                                              profile_date_created,
                          BTX.CREATION_TIME                                                                            date,
                          BTX.ID                                                                                       event_id,
                          mcab.CURRENCY                                                                                currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join balance.ACCOUNT mca ON mca.profile_id = up.id
                            join BALANCE.BALANCE mcab on mca.ID = mcab.ACCOUNT_ID
                            join BALANCE.TRANSACTION BTX ON BTX.ACCOUNT_ID = mca.ID
                   where true
                     and profile_date_created >= '2017-01-01'
                     and BTX.STATE = 'COMPLETED'
                     AND BTX.TYPE != 'DEPOSIT'

                   qualify row_number() over (partition by up.id order by BTX.CREATION_TIME) = 1
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
                 '5. CCY TX WITHIN 90 DAYS (EXCL DEPOSIT)' as event,
                 event_id::string
          from (
                   select up.id                                                                                        profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                              profile_type,
                          a.COUNTRY_CODE                                                                               country_code,
                          date_trunc('month', up.DATE_CREATED)                                                         cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                                          cohort_week,
                          up.date_created                                                                              profile_date_created,
                          BTX.CREATION_TIME                                                                            date,
                          BTX.ID                                                                                       event_id,
                          mcab.CURRENCY                                                                                currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join balance.ACCOUNT mca ON mca.profile_id = up.id
                            join BALANCE.BALANCE mcab on mca.ID = mcab.ACCOUNT_ID
                            join BALANCE.TRANSACTION BTX ON BTX.ACCOUNT_ID = mca.ID
                   where true
                     and profile_date_created >= '2017-01-01'
                     and BTX.STATE = 'COMPLETED'
                     AND BTX.TYPE != 'DEPOSIT'

                   qualify row_number() over (partition by up.id order by BTX.CREATION_TIME) = 1
               )
          where date is not null
            and date > DATEADD(day, 30, profile_date_created)
            and date < DATEADD(day, 90, profile_date_created)
      )

     ));