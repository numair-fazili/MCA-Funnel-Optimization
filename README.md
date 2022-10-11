## MCA-Funnel-Optimization
Optimisation for MCA Funnel

##Objective

Create MCA funnels to study the journey of newly onboarded customers. There are three funnels for each of balance (pending), account and spend (card) journeys.  


### Sanity Tests for MCA Account 

```
WITH PROFILE_COUNT AS (SELECT COUNT(EVENT_ID) as profileCount FROM SANDBOX_DB.SANDBOX_NUMAIR_FAZILI.mca_account_funnel WHERE EVENT = '1. PROFILE_CREATED' )

SELECT EVENT,COUNT(EVENT_ID), COUNT(EVENT_ID) / (SELECT profileCount from PROFILE_COUNT)*100 FROM SANDBOX_DB.SANDBOX_NUMAIR_FAZILI.mca_account_funnel
GROUP BY EVENT
ORDER BY EVENT
```

### Sanity Tests for MCA Card 

```
WITH PROFILE_COUNT AS (SELECT COUNT(EVENT_ID) as profileCount FROM SANDBOX_DB.{SANDBOX}.MCA_CARD_FUNNEL WHERE EVENT = '1. PROFILE_CREATED' )

SELECT EVENT,COUNT(EVENT_ID), COUNT(EVENT_ID) / (SELECT profileCount from PROFILE_COUNT)*100 FROM SANDBOX_DB.{SANDBOX}.MCA_CARD_FUNNEL
GROUP BY EVENT
ORDER BY EVENT
```

