CREATE
OR REPLACE TABLE {{params.reports}}.full_mca_funnel AS(
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
from (with ccy as (select distinct CURRENCY
                   from DEPOSITACCOUNT.DEPOSIT_ACCOUNT da
                            left join DEPOSITACCOUNT.BANK dab on dab.ID = da.BANK_ID
                   where SETTLEMENT_TYPE = 'BALANCE') -- What is escrow in  SETTLEMENT_TYPE from DEPOSITACCOUNT.DEPOSIT_ACCOUNT;  any significant use case?
          (
              select up.id                                                                                  profile_id,
                     IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                         'Business')                                                                        profile_type,
                     a.COUNTRY_CODE                                                                         country_code,
                     ccy.CURRENCY                                                                          currency,
                     date_trunc('month', up.DATE_CREATED)                                                   cohort_month,
                     date_trunc('week', up.DATE_CREATED)                                                    cohort_week,
                     up.date_created                                                                        profile_date_created,
                     up.DATE_CREATED                                                                        date,
                     '1. PROFILE_CREATED' as                                                                event,
                     up.id::string                                                                          event_id

              from PROFILE.USER_PROFILE up
                       cross join ccy  -- why is profile crossed with deposit currency account? - can be removed?
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
                     and CURRENCY in ('EUR','DKK','PLN','SEK','GBP','AUD','MYR','CAD','NZD','SGD','HUF','TRY','USD','RON','NOK') -- why are these hardcoded? - potentially because account details can only be issued for certain currencies - even then should be used as a param instead of hardcoded values
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
                 '3. BANK_DETAIL_REQUESTED' as event,
                 event_id::string
          from (
                   select up.id                                profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                      profile_type,
                          a.COUNTRY_CODE                       country_code,
                          date_trunc('month', up.DATE_CREATED) cohort_month,
                          date_trunc('week', up.DATE_CREATED)  cohort_week,
                          up.date_created                      profile_date_created,
                          bdos.BANKDETAIL_INITIATION_TIME      date,
                          bdos.order_id                        event_id,
                          bdos.CURRENCY                        currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join REPORTS.REPORT_FC_BANK_DETAILS_ORDER_STATUS bdos ON bdos.profile_id = up.id -- This is only for tracking request for bank details - the outcome of this step is outlined in 6 : Bank details issued
                   where 1=1
                     and profile_date_created >= '2017-01-01'
                     and CURRENCY in ('EUR','DKK','PLN','SEK','GBP','AUD','MYR','CAD','NZD','SGD','HUF','TRY','USD','RON','NOK')
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
                 '4. TOPPED UP OR PAID FEE' as event,
                 event_id::string
          from (
                   select up.id                                                                            profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                  profile_type,
                          a.COUNTRY_CODE                                                                   country_code,
                          date_trunc('month', up.DATE_CREATED)                                             cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                              cohort_week,
                          up.date_created                                                                  profile_date_created,
                          coalesce(TOPUP_COMPLETION_TIME, FEE_COMPLETION_TIME, BANKDETAIL_INITIATION_TIME) date,
                          bdos.order_id                                                                    event_id,
                          bdos.CURRENCY                                                                    currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join REPORTS.REPORT_FC_BANK_DETAILS_ORDER_STATUS bdos ON bdos.profile_id = up.id
                   where true
                     and profile_date_created >= '2017-01-01'
                     and CURRENCY in ('EUR','DKK','PLN','SEK','GBP','AUD','MYR','CAD','NZD','SGD','HUF','TRY','USD','RON','NOK')
                     and (TOPUP_STATUS = 'DONE' or TOPUP_STATUS is null) and
                          (FEE_STATUS = 'DONE' or FEE_STATUS is null)
     -- refactor to and (TOPUP_STATUS = 'DONE' or FEE_STATUS = 'DONE')

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
                 '5. VERIFIED' as event,
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
                     and CURRENCY in ('EUR','DKK','PLN','SEK','GBP','AUD','MYR','CAD','NZD','SGD','HUF','TRY','USD','RON','NOK')
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
                 '6. BANK DETAILS ISSUED' as event, -- Follow up on step 3
                 event_id::string
          from (
                   select up.id                                                                           profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                 profile_type,
                          a.COUNTRY_CODE                                                                  country_code,
                          date_trunc('month', up.DATE_CREATED)                                            cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                             cohort_week,
                          up.date_created                                                                 profile_date_created,
                          ALLOCATION_TIME                                                                 date,
                          da.id                                                                           event_id,
                          b.CURRENCY                                                                      currency,
                          row_number()
                                  over (partition by up.id,b.CURRENCY order by da.ALLOCATION_TIME asc) as currency_rank
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join DEPOSITACCOUNT.DEPOSIT_ACCOUNT da
                                 ON da.profile_id = up.id and SETTLEMENT_TYPE = 'BALANCE'
                            join DEPOSITACCOUNT.BANK b on da.BANK_ID = b.id
                   where true
                     and profile_date_created >= '2017-01-01'
                     and CURRENCY in ('EUR','DKK','PLN','SEK','GBP','AUD','MYR','CAD','NZD','SGD','HUF','TRY','USD','RON','NOK')
               )
          where date is not null
            and date < DATEADD(day, 30, profile_date_created)
            and currency_rank = 1
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
                 '7. RECEIVED TO DETAILS' as event,  -- (AKA performed a tx - either receive or sent) TMK this means that user has performed a TX but the receive rank counts all the currency g if i PLACED 10 TX with USD and 1 with SGD, it gives be 2 records - should only return 1 irrespective of CUR
                 event_id::string
          from (
                   select up.id                                                                                        profile_id,
                          IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                              'Business')                                                                              profile_type,
                          a.COUNTRY_CODE                                                                               country_code,
                          date_trunc('month', up.DATE_CREATED)                                                         cohort_month,
                          date_trunc('week', up.DATE_CREATED)                                                          cohort_week,
                          up.date_created                                                                              profile_date_created,
                          rec.RECEIVAL_DATE                                                                            date,
                          rec.REQUEST_ID                                                                               event_id,
                          rec.BANK_DETAIL_CURRENCY                                                                     currency,
                          row_number()
                                  over (partition by up.id,rec.BANK_DETAIL_CURRENCY order by rec.RECEIVAL_DATE asc) as receive_rank  -- context ranking of tx for a currency (should only select the first tx irrespective of currency) - eg SELECT *,row_number() over (partition by rec.PROFILE_ID,rec.BANK_DETAIL_CURRENCY order by rec.RECEIVAL_DATE asc) as receive_rank from REPORTS.RECEIVE_TRANSACTIONS rec where PROFILE_ID = 13558636
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join REPORTS.RECEIVE_TRANSACTIONS rec on rec.PROFILE_ID = up.id
                   where true
                     and profile_date_created >= '2017-01-01'
                     and CURRENCY in ('EUR','DKK','PLN','SEK','GBP','AUD','MYR','CAD','NZD','SGD','HUF','TRY','USD','RON','NOK')
               )
          where date is not null
            and date < DATEADD(day, 30, profile_date_created)
            and receive_rank = 1
      )

     UNION ALL -- TO REVIEW
    (
        select profile_id,
                     profile_type,
                     country_code,
                     currency,
                     cohort_month,
                     cohort_week,
                     profile_date_created,
                     date,
                     '8. USED Send Money BEFORE' as event,
                     event_id::string
        from (

            WITH SENDMONEY AS (SELECT USER_PROFILE_ID,SUBMIT_TIME FROM FX.REQUEST)
            select
                 up.id                                profile_id,
                   IFF(up.class = 'com.transferwise.fx.user.PersonalUserProfile', 'Personal',
                       'Business')                      profile_type,
                   a.COUNTRY_CODE                       country_code,
                   date_trunc('month', up.DATE_CREATED) cohort_month,
                   date_trunc('week', up.DATE_CREATED)  cohort_week,
                   mcab.CURRENCY                        currency,
                   mcab.CREATION_TIME                   date,
                   up.DATE_CREATED                      profile_date_created,
                   mcab.id                              event_id,
                   IFF(mcab.CREATION_TIME > (SELECT MIN(SENDMONEY.SUBMIT_TIME) FROM SENDMONEY where SENDMONEY.USER_PROFILE_ID = a.USER_PROFILE_ID),true,false) sendMoneyFirst
            from profile.USER_PROFILE up
                     join profile.ADDRESS a
                          ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                     join balance.ACCOUNT mca ON mca.profile_id = up.id
                     join BALANCE.BALANCE mcab on mca.ID = mcab.ACCOUNT_ID
            where true -- placeholder text - no functional impact (ignore)
              and profile_date_created >= '2017-01-01'
              and CURRENCY in ('EUR', 'DKK', 'PLN', 'SEK', 'GBP', 'AUD', 'MYR', 'CAD', 'NZD', 'SGD', 'HUF', 'TRY', 'USD', 'RON',
                               'NOK') -- why are these hardcoded? - potentially because account details can only be issued for certain currencies - even then should be used as a param instead of hardcoded values
              qualify row_number() over (partition by up.id order by mcab.CREATION_TIME) = 1
              order by mcab.CREATION_TIME)

            where sendMoneyFirst = true
            and date is not null
            and date < DATEADD(day, 30, profile_date_created)

        )



     ));