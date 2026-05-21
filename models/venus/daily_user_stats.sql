{{ config(
    alias = 'daily_user_stats_v2'
    , materialized = 'incremental'
    , incremental_strategy = 'delete+insert'
    , unique_key = ['day']
    , properties = {
        "partitioned_by": "ARRAY['day']"
    }
    , post_hook = "ALTER TABLE {{ this }} SET PROPERTIES extra_properties = map_from_entries(ARRAY[ROW('dune.public', 'true')])"
)
}}

WITH timeseries AS (

    WITH user_date_range AS (
         SELECT
            user,
            chain AS blockchain,
            vtoken_address,
            vtoken,
            underlying_token,
            underlying_token_address,
            pool,
            DATE(MIN(evt_block_date)) AS start_date,
            CURRENT_DATE AS end_date
        FROM {{ ref('all_user_transactions_v2') }}
        GROUP BY 1,2,3,4,5,6,7
    )

    SELECT
        user,
        blockchain,
        vtoken_address,
        vtoken,
        underlying_token,
        underlying_token_address,
        pool,
        day
    FROM user_date_range r
    CROSS JOIN UNNEST(sequence(
        {% if is_incremental() %}
        start_date
        {% else %}
        start_date
        {% endif %}
        , end_date, interval '1' day)) AS t(day)

),

daily_transactions AS (
    SELECT
        user,
        vtoken_address,
        evt_block_date AS day,
        COALESCE(SUM(amount_token) FILTER(WHERE action = 'supply'), 0) AS daily_supply,
        COALESCE(SUM(amount_token) FILTER(WHERE action = 'borrow'), 0) AS daily_borrow,
        COALESCE(SUM(amount_token) FILTER(WHERE action = 'redeem'), 0) AS daily_redeem,
        COALESCE(SUM(amount_token) FILTER(WHERE action = 'repay'), 0) AS daily_repay,
        COALESCE(SUM(amount_token) FILTER(WHERE action = 'supply'), 0) - COALESCE(SUM(amount_token) FILTER(WHERE action = 'redeem'), 0) AS net_daily_supply,
        COALESCE(SUM(amount_token) FILTER(WHERE action = 'borrow'), 0) - COALESCE(SUM(amount_token) FILTER(WHERE action = 'repay'), 0) AS net_daily_borrow,
        COALESCE(SUM(amount_vtoken) FILTER(WHERE action = 'transfer in'), 0) AS transfer_in,
        COALESCE(SUM(amount_vtoken) FILTER(WHERE action = 'transfer out'), 0) AS transfer_out,
        COALESCE(SUM(amount_vtoken) FILTER(WHERE action = 'transfer in'), 0) - COALESCE(SUM(amount_vtoken) FILTER(WHERE action = 'transfer out'), 0) AS net_daily_flow,
        COUNT(DISTINCT tx_hash) FILTER(WHERE action IN ('supply', 'borrow', 'redeem', 'repay')) AS num_txs
    FROM {{ ref('all_user_transactions_v2') }}
    GROUP BY 1,2,3
),

daily_state AS (
    SELECT
        user,
        vtoken_address,
        day,
        SUM(daily_supply) OVER(PARTITION BY user, vtoken_address ORDER BY day) AS total_supply,
        SUM(daily_borrow) OVER(PARTITION BY user, vtoken_address ORDER BY day) AS total_borrow,
        SUM(net_daily_flow) OVER(PARTITION BY user, vtoken_address ORDER BY day) AS supply_balance_vtoken,
        LEAD(day) OVER(PARTITION BY user, vtoken_address ORDER BY day) AS next_update_day
    FROM daily_transactions
),

borrows AS (
    WITH borrow_balance AS (
        SELECT 'bnb' AS chain, borrower AS user, contract_address, evt_block_time, evt_tx_hash, evt_index, accountBorrows FROM venus_bnb.vbnb_v2_evt_borrow
        UNION ALL
        SELECT 'bnb' AS chain, borrower AS user, contract_address, evt_block_time, evt_tx_hash, evt_index, accountBorrows FROM venus_bnb.vbnb_v2_evt_repayborrow
        UNION ALL
        SELECT 'bnb' AS chain, borrower AS user, contract_address, evt_block_time, evt_tx_hash, evt_index, accountBorrows FROM venus_bnb.vbep20delegate_evt_borrow
        UNION ALL
        SELECT 'bnb' AS chain, borrower AS user, contract_address, evt_block_time, evt_tx_hash, evt_index, accountBorrows FROM venus_bnb.vbep20delegate_evt_repayborrow
        UNION ALL
        SELECT chain, borrower AS user, contract_address, evt_block_time, evt_tx_hash, evt_index, accountBorrows FROM venus_multichain.vToken_evt_borrow
        UNION ALL
        SELECT chain, borrower AS user, contract_address, evt_block_time, evt_tx_hash, evt_index, accountBorrows FROM venus_multichain.vToken_evt_RepayBorrow
        UNION ALL (
        -- BNB Core Pool BEP20: Borrow from bnb.logs (replaces vbep20_bnb_core)
        SELECT
            'bnb' AS chain,
            varbinary_substring(l.data, 13, 20) AS user, -- borrower = slot1
            l.contract_address,
            l.block_time AS evt_block_time,
            l.tx_hash AS evt_tx_hash,
            l.index AS evt_index,
            CAST(varbinary_to_uint256(varbinary_substring(l.data, 65, 32)) AS DOUBLE) AS accountBorrows -- slot3
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date(m.deployment_date)
            AND l.topic0 = 0x13ed6866d4e1ee6da46f845c46d7e54120883d75c5ea9a2dacc1c4ca8984ab80 -- Borrow(address,uint256,uint256,uint256)
        )
        UNION ALL (
        -- BNB Core Pool BEP20: RepayBorrow from bnb.logs (replaces vbep20_bnb_core)
        SELECT
            'bnb' AS chain,
            varbinary_substring(l.data, 45, 20) AS user, -- borrower = slot2
            l.contract_address,
            l.block_time AS evt_block_time,
            l.tx_hash AS evt_tx_hash,
            l.index AS evt_index,
            CAST(varbinary_to_uint256(varbinary_substring(l.data, 97, 32)) AS DOUBLE) AS accountBorrows -- slot4
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date(m.deployment_date)
            AND l.topic0 = 0x1a2a22cb034d26d1854bdc6666a5b91fe25efbbb5dcad3b0355478d6f5c362a1 -- RepayBorrow(address,address,uint256,uint256,uint256)
        )
    )

    SELECT
        *,
        LEAD(day) OVER (PARTITION BY chain, contract_address, user ORDER BY day) AS next_update_day
    FROM (
        SELECT
            chain,
            DATE_TRUNC('day', evt_block_time) AS day,
            user,
            contract_address,
            CAST(accountBorrows AS DOUBLE) AS borrow_balance,
            ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('day', evt_block_time), chain, user, contract_address ORDER BY evt_block_time DESC, evt_index DESC) AS rn
        FROM borrow_balance
    )
    WHERE rn = 1 --latest borrow balance of the day
),

vBEP20_markets AS (
    SELECT vtoken_contract_address, deployment_date FROM dune.xvslove_team.dataset_markets_all_chains WHERE blockchain = 'bnb' AND pool = 'Core' AND vtoken != 'vBNB'
),

fees_paid AS (
    SELECT * FROM query_5579782
),

activity_data AS (
    SELECT
        ts.user,
        ts.day,
        ts.blockchain,
        ts.vtoken_address,
        ts.vtoken,
        ts.underlying_token,
        ts.underlying_token_address,
        ts.pool,
        dt.num_txs,

        --Daily change
        dt.daily_supply,
        dt.daily_redeem,
        dt.daily_borrow,
        dt.daily_repay,
        dt.net_daily_supply,
        dt.net_daily_borrow,
        dt.net_daily_flow * m.exchange_rate AS net_daily_flow, --in underlying tokens

        --Cumulative supplied/borrowed
        ds.total_supply, --total amount supplied all time (doesn't account for any redeems)
        ds.total_borrow, --total amount borrowed all time (doesn't account for any repays or liquidations)

        --Latest balances
        ds.supply_balance_vtoken * m.exchange_rate AS supply_balance, --current supply balance in underlying tokens
        ROUND(ds.supply_balance_vtoken * m.exchange_rate * m.price, 3) AS supply_balance_usd, --current supply balance in $
        b.borrow_balance / POWER(10, m.underlying_token_decimals) AS borrow_balance, --current borrow balance in underlying tokens
        ROUND(b.borrow_balance / POWER(10, m.underlying_token_decimals) * m.price, 3) AS borrow_balance_usd, --current borrow balance in $

        -- Interest & fees paid
        CASE WHEN ts.day = i.day THEN COALESCE(i.interest_paid, 0) ELSE 0 END AS daily_interest_paid,
        CASE WHEN ts.day = i.day THEN COALESCE(i.interest_paid_usd, 0) ELSE 0 END AS daily_interest_paid_usd,
        COALESCE(i.cum_interest_paid, 0) AS cum_interest_paid,
        COALESCE(i.cum_interest_paid_usd, 0) AS cum_interest_paid_usd,
        CASE WHEN ts.day = i.day THEN COALESCE(i.reserve_fee_usd, 0) ELSE 0 END AS daily_reserve_fee_usd,
        COALESCE(i.cum_reserve_fee_usd, 0) AS cum_reserve_fee_usd,
        CASE WHEN ts.day = i.day THEN COALESCE(i.liquidation_fee_usd, 0) ELSE 0 END AS daily_liquidation_fee_usd,
        COALESCE(i.cum_liquidation_fee_usd, 0) AS cum_liquidation_fee_usd,


        -- Activity
        CASE WHEN COALESCE(daily_supply,0) > 0 OR COALESCE(daily_redeem,0) > 0 OR COALESCE(daily_supply,0) > 0 OR COALESCE(daily_repay,0) > 0 THEN 1 ELSE 0 END AS active,
        CASE WHEN COALESCE(daily_supply,0) > 0 THEN 1 ELSE 0 END AS active_supply,
        CASE WHEN COALESCE(daily_redeem,0) > 0 THEN 1 ELSE 0 END AS active_redeem,
        CASE WHEN COALESCE(daily_borrow,0) > 0 THEN 1 ELSE 0 END AS active_borrow,
        CASE WHEN COALESCE(daily_repay,0) > 0 THEN 1 ELSE 0 END AS active_repay

    FROM timeseries ts
        LEFT JOIN daily_transactions dt ON dt.user = ts.user AND dt.vtoken_address = ts.vtoken_address AND dt.day = ts.day
        LEFT JOIN daily_state ds ON ds.user = ts.user AND ds.vtoken_address = ts.vtoken_address AND ts.day >= ds.day AND (ts.day < ds.next_update_day OR ds.next_update_day IS NULL)
        LEFT JOIN {{ ref('daily_market_info_v2') }} m ON ts.day = m.day AND ts.vtoken_address = m.vtoken_address
        LEFT JOIN borrows b ON ts.vtoken_address = b.contract_address AND ts.user = b.user AND ts.day >= b.day AND (ts.day < b.next_update_day OR b.next_update_day IS NULL)
        LEFT JOIN fees_paid i ON ts.vtoken_address = i.contract_address AND ts.user = i.borrower AND ts.day >= i.day AND (ts.day < i.next_update_day OR i.next_update_day IS NULL)
),

prime AS (
    SELECT
        chain,
        day,
        user,
        prime_user,
        LEAD(day) OVER (PARTITION BY chain, user ORDER BY day) AS next_update_day
    FROM (
        SELECT
            chain,
            DATE_TRUNC('day', evt_block_time) AS day,
            user,
            prime_user,
            ROW_NUMBER() OVER(PARTITION BY chain, user, DATE_TRUNC('day', evt_block_time) ORDER BY evt_block_time DESC, evt_index DESC) rn
        FROM (
            SELECT chain, evt_block_time, evt_index, user, 1 AS prime_user FROM venus_multichain.Prime_evt_Mint
            UNION ALL
            SELECT chain, evt_block_time, evt_index, user, 0 AS prime_user FROM venus_multichain.Prime_evt_Burn
        )
    )
    WHERE rn = 1
),

xvs_vault_user AS (
    SELECT
        day,
        chain,
        user,
        xvs_staked_amount,
        next_update_day
    FROM query_5667382
),

emode_status AS (
    SELECT 'bnb' AS blockchain, * FROM query_5930024
),

final_data AS (
    SELECT
        a.*,
        COALESCE(p.prime_user, 0) AS prime_user,
        CASE WHEN xvs.user IS NOT NULL THEN 1 ELSE 0 END AS xvs_staker,
        CASE WHEN s.address IS NOT NULL THEN 1 ELSE 0 END AS safe_multisig,
        COALESCE(e.emode_flag, 0) AS emode_user,
        COALESCE(e.pool_id, 0) AS emode_group
    FROM activity_data a
        LEFT JOIN prime p ON a.blockchain = p.chain AND a.user = p.user AND a.day >= p.day AND (a.day < p.next_update_day OR p.next_update_day IS NULL)
        LEFT JOIN xvs_vault_user xvs ON a.blockchain = xvs.chain AND a.user = xvs.user AND a.day >= xvs.day AND (a.day < xvs.next_update_day OR xvs.next_update_day IS NULL) AND xvs_staked_amount > 0
        LEFT JOIN safe.safes_all s ON a.blockchain = s.blockchain AND a.user = s.address
        LEFT JOIN emode_status e ON a.blockchain = e.blockchain AND a.user = e.user AND a.day >= e.block_date AND (a.day < e.next_update_day OR e.next_update_day IS NULL)
    WHERE
        a.pool IS NOT NULL
        AND a.vtoken IS NOT NULL
        AND (COALESCE(supply_balance_usd, 0) > 0 OR COALESCE(borrow_balance_usd, 0) > 0 OR active = 1)
)

SELECT * FROM final_data
