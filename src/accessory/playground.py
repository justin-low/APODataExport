import polars as pl
import datetime as dt
#
# date_load_schema = {
#     "date": pl.Datetime,
#     "start": pl.Datetime,
#     "end": pl.Datetime,
#     "logged_start_id": pl.Int64,
#     "logged_start_datetime": pl.Datetime,
#     "logged_end_id": pl.Int64,
#     "logged_end_datetime": pl.Datetime
# }
# res_df = pl.read_csv("target_date_time_ranges_old.csv", schema=date_load_schema)
#
# with pl.Config(set_tbl_rows=100):
#     print(res_df)
#
batch_size = 100000
id_start = 2646581501
id_end = 2650683064
iter = 0

while id_start < id_end:

    max_id = min(id_start + batch_size, id_end)
    start_time = dt.datetime.now()

    # # Execute query for current batch
    # cursor.execute(sql_query, (id_start, max_id))
    # rows = cursor.fetchall()
    # if not rows:
    #     break
    #
    # # Convert rows to DataFrame and merge with main DataFrame
    # res = pl.DataFrame(rows, schema=data_types)
    # if res_df is None:
    #     res_df = res
    # else:
    #     res_df.extend(res)

    print(f'finish iteration {iter} start id: {id_start}; end id: {max_id}; '
          f'duration (minutes): {(dt.datetime.now() - start_time).total_seconds() / 60}')

    id_start = min(max_id + 1, id_end)
    iter += 1