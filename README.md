# Venus dbt Backfill

Temporary dbt project to backfill Venus Core tables with:
1. Full history from `DATE '2020-01-01'` (incremental filter expanded)
2. `vbep20delegate` tables added without contract address filter (replacing the old 2-contract filter)

## Tables

- `daily_market_info`
- `daily_market_stats`
- `all_user_transactions`
- `daily_user_stats`

## GitHub Repo Variables / Secrets

| Name | Type | Value |
|------|------|-------|
| `DUNE_API_KEY` | Secret | Your Dune API key |
| `DUNE_TEAM_NAME` | Variable | Your Dune username/namespace |

## Usage

Trigger via **Actions → dbt backfill run → Run workflow**.

After backfill is confirmed correct, restore the date filters in the original Venus-ZD/venus_dbt repo to `date_add('day', -2, current_date)`.
