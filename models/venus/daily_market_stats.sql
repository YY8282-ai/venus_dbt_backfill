{{ config(
    alias = 'daily_market_stats_v2'
    , materialized = 'incremental'
    , incremental_strategy = 'delete+insert'
    , unique_key = ['day']
    , properties = {
        "partitioned_by": "ARRAY['day']"
    }
    , post_hook = "ALTER TABLE {{ this }} SET PROPERTIES extra_properties = map_from_entries(ARRAY[ROW('dune.public', 'true')])"
)
}}

WITH daily_market_info AS (
    SELECT * FROM {{ ref('daily_market_info_v2') }}
),

vBEP20_markets AS (
    SELECT vtoken_contract_address, deployment_date FROM dune.xvslove_team.dataset_markets_all_chains WHERE blockchain = 'bnb' AND pool = 'Core' AND vtoken != 'vBNB'
),

/**** DAILY SUPPLY ****/

mints_and_redeems AS ( --all mint and redeem events
        SELECT 'bnb' AS chain, contract_address, evt_block_time, mintTokens AS amount FROM venus_bnb.vBNB_V2_evt_mint
        UNION ALL
        SELECT 'bnb' AS chain, contract_address, evt_block_time, -redeemTokens AS amount FROM venus_bnb.vBNB_V2_evt_redeem
        UNION ALL
        SELECT chain, contract_address, evt_block_time, mintTokens AS amount FROM venus_multichain.vToken_evt_mint
        UNION ALL
        SELECT chain, contract_address, evt_block_time, -redeemTokens AS amount FROM venus_multichain.vToken_evt_redeem
        UNION ALL
        SELECT 'bnb' AS chain, contract_address, evt_block_time, mintTokens AS amount FROM venus_bnb.vbep20delegate_evt_mint
        UNION ALL
        SELECT 'bnb' AS chain, contract_address, evt_block_time, mintTokens AS amount FROM venus_bnb.vbep20delegate_evt_mintbehalf
        UNION ALL
        SELECT 'bnb' AS chain, contract_address, evt_block_time, -redeemTokens AS amount FROM venus_bnb.vbep20delegate_evt_redeem
        UNION ALL (
        -- BNB Core Pool BEP20: Mint pre-upgrade from bnb.logs (< 2023-11-03, 3-param)
        SELECT
            'bnb' AS chain,
            l.contract_address,
            l.block_time AS evt_block_time,
            varbinary_to_uint256(varbinary_substring(l.data, 65, 32)) AS amount -- mintTokens slot3
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date(m.deployment_date)
            AND l.block_date < date('2023-11-03')
            AND l.topic0 = 0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f -- Mint(address,uint256,uint256)
        )
        UNION ALL (
        -- BNB Core Pool BEP20: Mint post-upgrade from bnb.logs (>= 2023-11-03, 4-param with totalSupply)
        SELECT
            'bnb' AS chain,
            l.contract_address,
            l.block_time AS evt_block_time,
            varbinary_to_uint256(varbinary_substring(l.data, 65, 32)) AS amount -- mintTokens slot3
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date('2023-11-03')
            AND l.topic0 = 0xb4c03061fb5b7fed76389d5af8f2e0ddb09f8c70d1333abbb62582835e10accb -- Mint(address,uint256,uint256,uint256)
        )
        UNION ALL (
        -- BNB Core Pool BEP20: MintBehalf pre-upgrade from bnb.logs (< 2023-11-03, 4-param)
        SELECT
            'bnb' AS chain,
            l.contract_address,
            l.block_time AS evt_block_time,
            varbinary_to_uint256(varbinary_substring(l.data, 97, 32)) AS amount -- mintTokens slot4
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date(m.deployment_date)
            AND l.block_date < date('2023-11-03')
            AND l.topic0 = 0x297989b84a5f5b82d2ee0c266504c19bd9b10b410f187dc72ca4b0f0faecb345 -- MintBehalf(address,address,uint256,uint256)
        )
        UNION ALL (
        -- BNB Core Pool BEP20: MintBehalf post-upgrade from bnb.logs (>= 2023-11-03, 5-param with totalSupply)
        SELECT
            'bnb' AS chain,
            l.contract_address,
            l.block_time AS evt_block_time,
            varbinary_to_uint256(varbinary_substring(l.data, 97, 32)) AS amount -- mintTokens slot4
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date('2023-11-03')
            AND l.topic0 = 0xa24ddbdf0865ba440d364725c062eac72a908faeedffc33fe64d4640c82e3ab8 -- MintBehalf(address,address,uint256,uint256,uint256)
        )
        UNION ALL (
        -- BNB Core Pool BEP20: Redeem pre-upgrade from bnb.logs (< 2023-11-03, 3-param)
        SELECT
            'bnb' AS chain,
            l.contract_address,
            l.block_time AS evt_block_time,
            -varbinary_to_uint256(varbinary_substring(l.data, 65, 32)) AS amount -- -redeemTokens slot3
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date(m.deployment_date)
            AND l.block_date < date('2023-11-03')
            AND l.topic0 = 0xe5b754fb1abb7f01b499791d0b820ae3b6af3424ac1c59768edb53f4ec31a929 -- Redeem(address,uint256,uint256)
        )
        UNION ALL (
        -- BNB Core Pool BEP20: Redeem post-upgrade from bnb.logs (>= 2023-11-03, 4-param with totalSupply)
        SELECT
            'bnb' AS chain,
            l.contract_address,
            l.block_time AS evt_block_time,
            -varbinary_to_uint256(varbinary_substring(l.data, 65, 32)) AS amount -- -redeemTokens slot3
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date('2023-11-03')
            AND l.topic0 = 0xbd5034ffbd47e4e72a94baa2cdb74c6fad73cb3bcdc13036b72ec8306f5a7646 -- Redeem(address,uint256,uint256,uint256)
        )
),

daily_supply AS ( --supply over time = mints - redeems
    SELECT
        chain,
        day,
        contract_address,
        SUM(net_daily) OVER(PARTITION BY chain, contract_address ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS vtoken_supply,
        LEAD(day) OVER (PARTITION BY chain, contract_address ORDER BY day) AS next_update_day
    FROM (
        SELECT
            chain,
            DATE_TRUNC('day', evt_block_time) AS day,
            contract_address,
            SUM(amount / POWER(10,8)) AS net_daily
        FROM mints_and_redeems
        GROUP BY 1,2,3
    )
),


/**** DAILY BORROWS ****/

borrows AS (
        SELECT 'bnb' AS chain, evt_block_time, contract_address, totalBorrows FROM venus_bnb.vbnb_v2_evt_accrueinterest
        UNION ALL
        SELECT 'bnb' AS chain, evt_block_time, contract_address, totalBorrows FROM venus_bnb.vbep20delegate_evt_accrueinterest
        UNION ALL (
        -- BNB Core Pool BEP20: AccrueInterest from bnb.logs (replaces vbep20_bnb_core)
        SELECT
            'bnb' AS chain,
            l.block_time AS evt_block_time,
            l.contract_address,
            varbinary_to_uint256(varbinary_substring(l.data, 97, 32)) AS totalBorrows -- slot4
        FROM vBEP20_markets m
        INNER JOIN bnb.logs l ON
            l.contract_address = m.vtoken_contract_address
            AND l.block_date >= date(m.deployment_date)
            AND l.topic0 = 0x4dec04e750ca11537cabcd8a9eab06494de08da3735bc8871cd41250e190bc04 -- AccrueInterest(uint256,uint256,uint256,uint256)
        )
        UNION ALL
        SELECT chain, evt_block_time, contract_address, totalBorrows FROM venus_multichain.vToken_evt_accrueinterest
),

daily_borrow as (
    SELECT
        *,
        LEAD(day) OVER (PARTITION BY chain, contract_address ORDER BY day) AS next_update_day
    FROM (
        SELECT
            chain,
            DATE_TRUNC('day', evt_block_time) AS day,
            contract_address,
            AVG(totalBorrows) AS token_borrows
        FROM borrows
        GROUP BY 1,2,3
    )
),


/**** DAILY INTEREST ****/

daily_interest AS (
    SELECT
        chain,
        day,
        contract_address,
        interest_raw,
        SUM(interest_raw) OVER (PARTITION BY chain, contract_address ORDER BY day) AS interest_raw_acum
    FROM (
            SELECT
                chain,
                DATE_TRUNC('day', evt_block_time) AS day,
                contract_address,
                SUM(interestAccumulated) AS interest_raw
            FROM (
                SELECT 'bnb' AS chain, evt_block_time, contract_address, interestAccumulated FROM venus_bnb.VBNB_V2_evt_AccrueInterest
                UNION ALL
                SELECT 'bnb' AS chain, evt_block_time, contract_address, interestAccumulated FROM venus_bnb.vbep20delegate_evt_accrueinterest
                UNION ALL (
                -- BNB Core Pool BEP20: AccrueInterest from bnb.logs (replaces vbep20_bnb_core)
                SELECT
                    'bnb' AS chain,
                    l.block_time AS evt_block_time,
                    l.contract_address,
                    varbinary_to_uint256(varbinary_substring(l.data, 33, 32)) AS interestAccumulated -- slot2
                FROM vBEP20_markets m
                INNER JOIN bnb.logs l ON
                    l.contract_address = m.vtoken_contract_address
                    AND l.block_date >= date(m.deployment_date)
                    AND l.topic0 = 0x4dec04e750ca11537cabcd8a9eab06494de08da3735bc8871cd41250e190bc04 -- AccrueInterest(uint256,uint256,uint256,uint256)
                )
                UNION ALL
                SELECT chain, evt_block_time, contract_address, interestAccumulated FROM venus_multichain.vToken_evt_AccrueInterest
            )
            GROUP BY 1,2,3
    )
),

/**** LIQUIDATIONS ****/

daily_liquidations AS (
    SELECT
        chain,
        evt_block_date AS day,
        vtoken_address,
        SUM(repay_amount_usd) AS repay_amount_usd,
        SUM(liquidation_protocol_fee_usd) AS liquidation_protocol_fee
    FROM query_5754116
    GROUP BY 1,2,3
)


/**** FINAL DATA ****/

SELECT
    *,
    token_supply - token_borrows AS token_liquidity,
    usd_supply - usd_borrows AS usd_liquidity,
    SUM(reserve_revenue) OVER (PARTITION BY blockchain, vtoken_address ORDER BY day) AS reserve_revenue_acum,
    SUM(liquidation_revenue) OVER (PARTITION BY blockchain, vtoken_address ORDER BY day) AS liquidation_revenue_acum
FROM (
    SELECT
        --market info
        m.blockchain,
        m.day,
        m.vtoken,
        m.vtoken_address,
        m.underlying_token,
        m.underlying_token_address,
        m.pool,
        --total daily supply
        s.vtoken_supply * m.exchange_rate AS token_supply,
        s.vtoken_supply * m.exchange_rate * m.price AS usd_supply,
        --total daily borrow
        b.token_borrows / POWER(10, m.underlying_token_decimals) AS token_borrows,
        b.token_borrows / POWER(10, m.underlying_token_decimals) * m.price AS usd_borrows,
        --reserve revenue
        CASE WHEN m.vtoken_address != 0x183dE3C349fCf546aAe925E1c7F364EA6FB4033c
            THEN COALESCE(i.interest_raw / POWER(10, m.underlying_token_decimals) * m.price * m.reserve_factor, 0)
        ELSE 0 END AS reserve_revenue,
        --liquidation revenue
        COALESCE(l.liquidation_protocol_fee, 0) AS liquidation_revenue

    FROM daily_market_info m
        LEFT JOIN daily_supply s       ON s.contract_address = m.vtoken_address AND m.blockchain = s.chain AND m.day >= s.day AND (m.day < s.next_update_day OR s.next_update_day IS NULL)
        LEFT JOIN daily_borrow b       ON b.contract_address = m.vtoken_address AND m.blockchain = b.chain AND m.day >= b.day AND (m.day < b.next_update_day OR b.next_update_day IS NULL)
        LEFT JOIN daily_interest i     ON i.contract_address = m.vtoken_address AND m.blockchain = i.chain AND m.day = i.day
        LEFT JOIN daily_liquidations l ON l.vtoken_address = m.vtoken_address AND m.blockchain = l.chain AND m.day = l.day

    WHERE vtoken_supply IS NOT NULL
    {% if is_incremental() %}
    AND m.day >= DATE '2020-01-01'
    {% endif %}
)

ORDER BY blockchain, day DESC, usd_supply DESC
