-- Baseline params

SET START_DATE = '2020-01-01'; -- used as a reference point of profile creation
SET WINDOW_PERIOD =  30; -- all subsequent days must be within this value to the profile creation date

CREATE
OR REPLACE TABLE {{params.reports}}.mca_account_funnel AS(
select profile_id as profile_id,
       currency as currency,
       profile_date_created as profile_date_created,
       date as date,
       event as event,
       event_id as event_id
from (
          (
              select up.id                                                                                  profile_id,
                     'N/A'                                                                                  currency,
                     up.date_created                                                                        profile_date_created,
                     up.DATE_CREATED                                                                        date,
                     '1. PROFILE_CREATED' as                                                                event,
                     up.id::string                                                                          event_id

              from PROFILE.USER_PROFILE up
                       join profile.ADDRESS a
                            ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS' -- what is the use of primary user profile address condition?
              where true
                and profile_date_created >= $START_DATE
          )

      UNION ALL -- finding users who opened a balance account (all balances)
      (
          select profile_id,
                 currency,
                 profile_date_created,
                 date,
                 '2. BALANCE_OPENED' as event,
                 event_id::string
          from (
                   select up.id                                profile_id,
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
                     and profile_date_created >= $START_DATE

                   qualify row_number() over (partition by up.id order by mcab.CREATION_TIME) = 1
               )
          where date is not null
            and date < DATEADD(day, $WINDOW_PERIOD, profile_date_created)
      )


      UNION ALL -- subset of above users who opened a balance account in currencies which have account details feature
      (
          select profile_id,
                 currency,
                 profile_date_created,
                 date,
                 '3. BALANCE_OPENED_CCY_WITH_ACC_DETAILS' as event,
                 event_id::string
          from (
                   select up.id                                profile_id,
                          mcab.CURRENCY                        currency,
                          mcab.CREATION_TIME                   date,
                          up.DATE_CREATED                      profile_date_created,
                          CONCAT(mcab.id,9999)                       event_id -- can potentially overlap with the key used above if first balance account = first balance account with account details feature
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join balance.ACCOUNT mca ON mca.profile_id = up.id
                            join BALANCE.BALANCE mcab on mca.ID = mcab.ACCOUNT_ID
                   where true -- placeholder text - no functional impact (ignore)
                     and profile_date_created >= $START_DATE
                     and CURRENCY in  ('GBP','EUR','USD','AUD','NZD','SGD','RON','CAD','HUF','TRY')-- Account details are only available for these ccy's

                   qualify row_number() over (partition by up.id order by mcab.CREATION_TIME) = 1
               )
          where date is not null
            and date < DATEADD(day, $WINDOW_PERIOD, profile_date_created)
      )

      UNION ALL -- finding users who requested for bank details (note that users can open a balance account but not request for account details. This use case is analogous to holding currencies.
      (
          select profile_id,
                 currency,
                 profile_date_created,
                 date,
                 '4. BANK_DETAIL_REQUESTED' as event,
                 event_id::string
          from (
                   select up.id                                profile_id,

                          up.date_created                      profile_date_created,
                          bdos.BANKDETAIL_INITIATION_TIME      date,
                          bdos.order_id                        event_id,
                          bdos.CURRENCY                        currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join REPORTS.REPORT_FC_BANK_DETAILS_ORDER_STATUS bdos ON bdos.profile_id = up.id -- This is only for tracking request for bank details - the outcome of this step is outlined in 6 : Bank details issued
                   where true
                     and profile_date_created >= $START_DATE
                     and bdos.CURRENCY in  ('GBP','EUR','USD','AUD','NZD','SGD','RON','CAD','HUF','TRY')

                   qualify row_number() over (partition by up.id order by bdos.BANKDETAIL_INITIATION_TIME) = 1
               )
          where date is not null
            and date < DATEADD(day, $WINDOW_PERIOD, profile_date_created)
      )

      UNION ALL -- filtering verified users . To note that, top up and fee are not included in the funnel as these requirements were not originally part of MCA flow.
      (
          select profile_id,
                 currency,
                 profile_date_created,
                 date,
                 '5. VERIFIED' as event,
                 event_id::string
          from (
                   select up.id                                                     profile_id,
                          up.date_created                                           profile_date_created,
                          coalesce(VERIFICATION_COMPLETION_TIME, BANKDETAIL_INITIATION_TIME) date,
                          bdos.order_id                                             event_id,
                          bdos.CURRENCY                                             currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join REPORTS.REPORT_FC_BANK_DETAILS_ORDER_STATUS bdos ON bdos.profile_id = up.id
                   where true
                     and profile_date_created >= $START_DATE
                     and CURRENCY in  ('GBP','EUR','USD','AUD','NZD','SGD','RON','CAD','HUF','TRY')
                     and (VERIFICATION_STATUS = 'DONE')

                   qualify row_number() over (partition by up.id order by coalesce(VERIFICATION_COMPLETION_TIME, BANKDETAIL_INITIATION_TIME)) = 1
               )
          where date is not null
            and date < DATEADD(day, $WINDOW_PERIOD, profile_date_created)
      )
      UNION ALL -- filtering users for which bank details were issued
      (
              select profile_id,
                 currency,
                 profile_date_created,
                 date,
                 '6. BANK DETAILS ISSUED' as event, -- Follow up on step 3
                 event_id::string
              from (
                   select up.id                                                                           profile_id,
                          up.date_created                                                                 profile_date_created,
                          bdos.BANKDETAIL_ISSUANCE_TIME                                                   date,
                          CONCAT(bdos.ORDER_ID,9999)                                                      event_id, -- clashes with previous - concat with 9999 for uniqueness
                          bdos.CURRENCY                                                                      currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                            join REPORTS.REPORT_FC_BANK_DETAILS_ORDER_STATUS bdos ON bdos.profile_id = up.id
                   where true
                     and profile_date_created >= $START_DATE
                     and CURRENCY in  ('GBP','EUR','USD','AUD','NZD','SGD','RON','CAD','HUF','TRY')
                     and bdos.BANK_DETAILS_ISSUED = true
                   qualify row_number() over (partition by up.id order by bdos.BANKDETAIL_ISSUANCE_TIME) = 1)
              where date is not null
                and date < DATEADD(day, $WINDOW_PERIOD, profile_date_created)
      )

      UNION ALL -- filtering users who performed a cross currency TX
      (
              select profile_id,
                 currency,
                 profile_date_created,
                 date,
                 '7. PERFORMED_CROSS_CCY_TX' as event,
                 event_id::string
              from (
                   select up.id                                                                                        profile_id,
                          up.date_created                                                                              profile_date_created,
                          report_action_step.action_completion_time                                                    date,
                          report_action_step.ACTION_ID                                                                 event_id,
                          report_action_step.target_currency                                                           currency
                   from profile.USER_PROFILE up
                            join profile.ADDRESS a
                                 ON up.ID = a.USER_PROFILE_ID and a.ADDRESS_TYPE = 'PRIMARY_USER_PROFILE_ADDRESS'
                   INNER JOIN BALANCE.ACCOUNT  AS mca ON mca.PROFILE_ID = up.ID
                   LEFT JOIN reports.report_action_step  AS report_action_step ON report_action_step.user_profile_id = mca.profile_id AND (report_action_step.successful_action = 1) = TRUE AND report_action_step.product_type IN ('BALANCE')
                   WHERE true
                     and (report_action_step.source_currency != report_action_step.target_currency ) AND (report_action_step.flag_for_aggregations = 1  )
                     and profile_date_created >= $START_DATE
                     and report_action_step.SOURCE_CURRENCY in  ('GBP','EUR','USD','AUD','NZD','SGD','RON','CAD','HUF','TRY')

                   qualify row_number() over (partition by up.id order by report_action_step.action_completion_time) = 1)
              where date is not null
                and date < DATEADD(day, $WINDOW_PERIOD, profile_date_created)
      )

      UNION ALL -- filtering users who received money in MCA account. This may exceed the previous event as it is contingent on how customers use the account.
      (
              select profile_id,
                 currency,
                 profile_date_created,
                 date,
                 '8. RECEIVED_TO_DETAILS' as event,
                 event_id::string
              from (
                   select up.id                                                                                        profile_id,
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
                     and profile_date_created >= $START_DATE
                     and CURRENCY in  ('GBP','EUR','USD','AUD','NZD','SGD','RON','CAD','HUF','TRY'))
              where date is not null
                and date < DATEADD(day, $WINDOW_PERIOD, profile_date_created)
                and receive_rank = 1
      )


     ));