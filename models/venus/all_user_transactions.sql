{{ config(
    alias = 'all_user_transactions_v2'
    , materialized = 'incremental'
    , incremental_strategy = 'delete+insert'
    , unique_key = ['evt_block_date']
    , properties = {
        "partitioned_by": "ARRAY['evt_block_date']"
    }
    , post_hook = "ALTER TABLE {{ this }} SET PROPERTIES extra_properties = map_from_entries(ARRAY[ROW('dune.public', 'true')])"
)
}}

WITH daily_market_info AS (
    SELECT * FROM {{ ref('daily_market_info_v2') }}
),

tokens AS (
    SELECT DISTINCT blockchain, vToken_address FROM daily_market_info
),

borrows AS (
    SELECT
      'borrow' AS action,
      chain,
      evt_block_time,
      evt_block_number,
      borrower AS user,
      contract_address AS vtoken_address,
      evt_tx_hash AS tx_hash,
      borrowAmount AS amount,
      NULL AS amount_vtoken
    FROM (
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, borrower, contract_address, evt_tx_hash, borrowAmount FROM venus_bnb.VBNB_V2_evt_Borrow
        UNION ALL
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, borrower, contract_address, evt_tx_hash, borrowAmount FROM venus_bnb.vbep20delegate_evt_borrow
        UNION ALL
        SELECT chain, evt_block_time, evt_block_number, borrower, contract_address, evt_tx_hash, borrowAmount FROM venus_multichain.vtoken_evt_borrow
        ) a
    WHERE borrowAmount > 0
),

repays AS (
    SELECT
      'repay' AS action,
      chain,
      evt_block_time,
      evt_block_number,
      borrower AS user,
      contract_address AS vtoken_address,
      evt_tx_hash AS tx_hash,
      repayAmount AS amount,
      NULL AS amount_vtoken
    FROM (
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, borrower, contract_address, evt_tx_hash, repayAmount FROM venus_bnb.VBNB_V2_evt_repayborrow
        UNION ALL
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, borrower, contract_address, evt_tx_hash, repayAmount FROM venus_bnb.vbep20delegate_evt_repayborrow
        UNION ALL
        SELECT chain, evt_block_time, evt_block_number, borrower, contract_address, evt_tx_hash, repayAmount FROM venus_multichain.vtoken_evt_repayborrow
        ) a
    WHERE repayAmount > 0
),

mints AS (
    SELECT
      'supply' AS action,
      chain,
      evt_block_time,
      evt_block_number,
      minter AS user,
      contract_address AS vtoken_address,
      evt_tx_hash AS tx_hash,
      mintAmount AS amount,
      mintTokens AS amount_vtoken
    FROM (
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, minter, contract_address, evt_tx_hash, mintAmount, mintTokens FROM venus_bnb.VBNB_V2_evt_mint
        UNION ALL
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, minter, contract_address, evt_tx_hash, mintAmount, mintTokens FROM venus_bnb.vbep20delegate_evt_mint
        UNION ALL
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, receiver, contract_address, evt_tx_hash, mintAmount, mintTokens FROM venus_bnb.vbep20delegate_evt_mintbehalf
        UNION ALL
        SELECT chain, evt_block_time, evt_block_number, minter, contract_address, evt_tx_hash, mintAmount, mintTokens FROM venus_multichain.vtoken_evt_mint
    ) a
    WHERE mintAmount > 0
),

redeems AS (
    SELECT
      'redeem' AS action,
      chain,
      evt_block_time,
      evt_block_number,
      redeemer AS user,
      contract_address AS vtoken_address,
      evt_tx_hash AS tx_hash,
      redeemAmount AS amount,
      redeemTokens AS amount_vtoken
    FROM (
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, redeemer, contract_address, evt_tx_hash, redeemAmount, redeemTokens FROM venus_bnb.VBNB_V2_evt_redeem
        UNION ALL
        SELECT 'bnb' AS chain, evt_block_time, evt_block_number, redeemer, contract_address, evt_tx_hash, redeemAmount, redeemTokens FROM venus_bnb.vbep20delegate_evt_redeem
        UNION ALL
        SELECT chain, evt_block_time, evt_block_number, redeemer, contract_address, evt_tx_hash, redeemAmount, redeemTokens FROM venus_multichain.vtoken_evt_redeem
    ) a
    WHERE redeemAmount > 0
),

--the vtoken decoded contracts don't trace events all the way back, so for accurate balance calculation need to use the token transfer tables.
transfers AS (
    SELECT
          'transfer in' AS action,
          chain,
          block_time AS evt_block_time,
          block_number,
          "to" AS user,
          contract_address AS vtoken_address,
          tx_hash,
          NULL AS amount,
          amount AS amount_vtoken
    FROM (
        SELECT 'bnb' AS chain, block_time, block_number, to, contract_address, tx_hash, amount FROM tokens_bnb.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'bnb')
        UNION ALL
        SELECT 'arbitrum' AS chain, block_time, block_number, to, contract_address, tx_hash, amount FROM tokens_arbitrum.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'arbitrum')
        UNION ALL
        SELECT 'base' AS chain, block_time, block_number, to, contract_address, tx_hash, amount FROM tokens_base.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'base')
        UNION ALL
        SELECT 'ethereum' AS chain, block_time, block_number, to, contract_address, tx_hash, amount FROM tokens_ethereum.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'ethereum')
        UNION ALL
        SELECT 'opbnb' AS chain, block_time, block_number, to, contract_address, tx_hash, amount FROM tokens_opbnb.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'opbnb')
        UNION ALL
        SELECT 'optimism' AS chain, block_time, block_number, to, contract_address, tx_hash, amount FROM tokens_optimism.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'optimism')
        UNION ALL
        SELECT 'unichain' AS chain, block_time, block_number, to, contract_address, tx_hash, amount FROM tokens_unichain.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'unichain')
        UNION ALL
        SELECT 'zksync' AS chain, block_time, block_number, to, contract_address, tx_hash, amount FROM tokens_zksync.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'zksync')
        ) a
    WHERE amount > 0 AND "to" NOT IN (contract_address, 0x0000000000000000000000000000000000000000)

    UNION ALL

    SELECT
          'transfer out' AS action,
          chain,
          block_time AS evt_block_time,
          block_number,
          "from" AS user,
          contract_address AS vtoken_address,
          tx_hash,
          NULL AS amount,
          amount AS amount_vtoken
    FROM (
        SELECT 'bnb' AS chain, block_time, block_number, "from", contract_address, tx_hash, amount FROM tokens_bnb.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'bnb')
        UNION ALL
        SELECT 'arbitrum' AS chain, block_time, block_number, "from", contract_address, tx_hash, amount FROM tokens_arbitrum.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'arbitrum')
        UNION ALL
        SELECT 'base' AS chain, block_time, block_number, "from", contract_address, tx_hash, amount FROM tokens_base.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'base')
        UNION ALL
        SELECT 'ethereum' AS chain, block_time, block_number, "from", contract_address, tx_hash, amount FROM tokens_ethereum.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'ethereum')
        UNION ALL
        SELECT 'opbnb' AS chain, block_time, block_number, "from", contract_address, tx_hash, amount FROM tokens_opbnb.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'opbnb')
        UNION ALL
        SELECT 'optimism' AS chain, block_time, block_number, "from", contract_address, tx_hash, amount FROM tokens_optimism.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'optimism')
        UNION ALL
        SELECT 'unichain' AS chain, block_time, block_number, "from", contract_address, tx_hash, amount FROM tokens_unichain.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'unichain')
        UNION ALL
        SELECT 'zksync' AS chain, block_time, block_number, "from", contract_address, tx_hash, amount FROM tokens_zksync.transfers WHERE contract_address IN (SELECT vToken_address FROM tokens WHERE blockchain = 'zksync')
        ) a
    WHERE amount > 0 AND "from" NOT IN (contract_address, 0x0000000000000000000000000000000000000000)
),

all_txs AS (
    SELECT * FROM borrows
    UNION ALL
    SELECT * FROM repays
    UNION ALL
    SELECT * FROM mints
    UNION ALL
    SELECT * FROM redeems
    UNION ALL
    SELECT * FROM transfers
),

emode_status AS (
    SELECT 'bnb' AS blockchain, * FROM query_5930024
)

SELECT
    t.action,
    t.chain,
    t.evt_block_time,
    DATE_TRUNC('day', t.evt_block_time) AS evt_block_date,
    t.evt_block_number,
    t.user,
    t.tx_hash,
    t.vtoken_address,
    m.vtoken,
    m.pool,
    m.underlying_token,
    m.underlying_token_address,
    amount / POWER(10, m.underlying_token_decimals) AS amount_token,
    amount / POWER(10, m.underlying_token_decimals) * COALESCE(p.price, m.price) AS amount_usd, --if price not in table, take daily price from market information table
    amount_vtoken,
    COALESCE(p.price, m.price) AS price,
    m.exchange_rate,
    COALESCE(e.emode_flag, 0) AS emode_user,
    COALESCE(e.pool_id, 0) AS emode_id,
    e.pool_name AS emode_label
FROM all_txs t
LEFT JOIN daily_market_info m ON DATE_TRUNC('day', t.evt_block_time) = m.day AND t.vtoken_address = m.vtoken_address
LEFT JOIN (
    SELECT
        blockchain, contract_address, symbol, timestamp,
        CASE
            WHEN symbol = 'xSolvBTC' AND blockchain = 'bnb' AND price > 1e6 THEN NULL -- upstream prices.minute spike 4/27-4/30; falls back to m.price
            ELSE price
        END AS price
    FROM prices.minute
    {% if is_incremental() %}WHERE timestamp >= DATE '2020-01-01'{% endif %}
) p ON t.chain = p.blockchain AND DATE_TRUNC('minute', t.evt_block_time) = p.timestamp AND m.underlying_token_address = p.contract_address
LEFT JOIN emode_status e ON e.blockchain = t.chain AND e.user = t.user AND t.evt_block_number >= e.block_number AND (t.evt_block_number < e.next_update_block OR e.next_update_block IS NULL)
{% if is_incremental() %}
WHERE DATE_TRUNC('day', t.evt_block_time) >= DATE '2020-01-01'
{% endif %}
