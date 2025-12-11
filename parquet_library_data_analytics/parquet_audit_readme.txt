Остались без измеений, как и были предоставлены:
parquet_reader_lib.h
parquet_reader_lib.cpp
parquet_reader.cpp

parquet2csv.cpp конвертирует паркет в csv, не используется в далнейшем коде, просто служит конвертором,чтобы можно было просмотрть файл 

1. Анализ файлов trade_spot:
parquet_trade_spot_audit.cpp:
Scan a directory of parquet files and detect anomalies.
    3 // By default writes to output NDJSON only files that *have anomalies*.
    4 // Use optional flag --all to write all files.
    6 // Build:
    7 //   g++ -std=gnu++23 -O3 parquet_trade_spot_audit.cpp -lparquet -larrow -lzstd -o parquet_trade_spot_audit
    8 //
    9 // Usage:
   10 //   ./parquet_trade_spot_audit /path/to/parquet_dir anomalies.ndjson        # only anomalous files
   11 //   ./parquet_trade_spot_audit /path/to/parquet_dir anomalies_all.ndjson --all  # all files

Что именно считается аномалией в этом коде:
- rows_scanned == 0 — файл не читается или полностью пуст (очень подозрительно).
- rows_scanned != meta_rows — реальное число строк не совпадает с числом в метаданных.
dup_tradeid > 0 — дубли tradeId внутри одного файла (возможная повторная запись/повторная загрузка).
null_counts > 0 — есть отсутствующие/недостающие значения в колонках ts, px, qty, tradeId.
non_monotonic_ts > 0 — временные метки идут назад (невозмозможно для корректной последовательности).
rows_scanned < 90% of meta_rows — сильное несоответствие размера (файл усечён).
meta_rows < 100 — слишком маленький файл (вероятно незагруженный кусок).
Статистические выбросы (z-score > 3) по следующим метрикам:
rows_ratio (rows_scanned/meta_rows),
max_gap_ns (максимальная пауза между соседними ts),
gap_mean (средняя пауза),
px_avg (средняя цена),
qty_avg (средняя величина).
По этим параметрам выявляются файлы, которые статистически выбиваются из общей популяции (по	лезно, если скачивание иногда даёт смещённые куски).

Аномалии отфильтрованы в anomalies_trade_spots.ndjson: по каждому паркет файлу, в котором выявлена какая-либо аномалия, показан анализ и в конце указаы какие именно аномалии выявлены (например, "anomalies":["rows_scanned == 0"])

2. Анализ файлов depth_spot:
 parquet_depth_audit.cpp
// Build:
//   g++ -std=gnu++23 -O3 parquet_depth_audit.cpp -lparquet -larrow -lzstd -o parquet_depth_audit
// Usage:
//   ./parquet_depth_audit /path/to/parquets anomalies_depth.ndjson	# only anomalous files
//   ./parquet_depth_audit /path/to/parquets anomalies_depth_all.ndjson --all   # to write all files
//
// Notes:
// - Focus on depth-style parquet containing columns:
//    ts, firstId, lastId, eventTime, bid.list.element.px, bid.list.element.qty,
//    ask.list.element.px, ask.list.element.qty
// - If bid/ask are flattened (repeated physical values) without offsets, we will compute global stats
//   but won't be able to map per-row arrays to rows. The tool will flag that as informational anomaly.

Что именно считается аномалиями для depth-файлов:
Явные/структурные аномалии
- rows_scanned == 0 — файл пустой или не удалось прочитать row-groups. Чаще всего говорит о повреждённом или неполном файле.
- rows_scanned != meta_rows — метаданные Parquet предлагают одно число строк, а фактическое чтение — другое. Возможна частичная загрузка, битый row-group, или проблемы при записи.
- null_counts > 0 для ts, firstId, lastId, eventTime — важные скалярные поля отсутствуют у каких-то записей → файл неполный/битый.
+ non_monotonic_ts > 0 — временные метки идут назад: серьёзный индикатор проблем с порядком или с агрегатором.
+ lastId < firstId (в строке) — внутри строки lastId меньше firstId — логически неверно.
+ id_overlap_count > 0 (т. е. firstId <= prev.lastId) — диапазоны firstId..lastId перекрываются с предыдущими — возможная дубликация/повторная запись при скачивании.
+ id_gap_count > 0 (т. е. firstId > prev.lastId + 1) — пропуск id: между соседними строчками есть разрыв ID — пропущенные delta-сегменты.
Данные по книге (bid/ask):
+ has_bid_px but bid_px_count==0 и аналогично для ask — колонка заявлена, но данных нет — подозрительно.
high_fraction_bid_qty_zero / high_fraction_ask_qty_zero (>10%) — большое число записей с qty==0
— qty==0 может означать удаление ордеров (в корректном delta-потоке это возможно), но если доля высока — это необычно и стоит проверить.
+ high_fraction_bid_px_zero / high_fraction_ask_px_zero (>10%) — цены, равные 0 — почти наверняка ошибка (цена не должна быть 0).
per-row bid/ask counts mismatch rows_scanned — если паркет содержит per-row поля для bid_px/ask_px, но их количество не равняется числу строк — явная несогласованность.
первыйй бид < первый аск в одной строке
цена не может меняться больше в 10 раз
количество в любой строке не может отличаться от среднего больше чем в 1млн раз	
цена должна делиться еа 1тыс., количество на 100млн: Указана в минимальных единицах (сатоши, 1e-8).Например: 3204000 = 0.03204 (BTCUSDT).Тоже в минимальных единицах (сатоши).Пример: 871000000000 = 8710 BTC.

+ тольо для иди тп: flattened_bid_or_ask_arrays_without_offsets (informational) — parquet содержит повторяющиеся элементы (flattened arrays) но нет offset/indices колонки, поэтому нельзя сопоставить элементы массивов с конкретными строками — это мешает детальному per-row auditing; отмечается как информационный «анализ-ограничение».
Гибкие/эвристические:
meta_rows < 10 (или <100 ) — очень маленький файл: возможно, неполный кусок.

Аномалии отфильтрованы в anomalies_depth_all.ndjson: по каждому паркет файлу, в котором выявлена какая-либо аномалия, показан анализ и в конце указаны какие именно аномалии выявлены (например, "anomalies":["high_fraction_bid_qty_zero","high_fraction_ask_qty_zero"]})

Кроме аномалий, которые попали в файл, выдало ошибки, которые в файл не попадают (запись в NDJSON выполняется только для успешно прочитанных файлов (либо только аномальных, либо — с --all — для всех успешно прочитанных), например:
ERROR: open/read failed for /path/to/bn_depth_spot_DFUSDT_2024_10_17.parquet : Unexpected end of stream: Read 0 values, expected 1570
Они означают, что паркет-файл повреждён/обрезан и libparquet не смог дочитать ожидаемые значения. А ещё — такие файлы в текущей реализации не попадают в выходной NDJSON (утилита печатает ошибку в stderr и пропускает файл), поэтому их нужно либо искать в логах stderr, либо изменить программу, чтобы она явно записывала ошибки в результирующий JSON.
Подробнее: Parquet/column reader ожидал прочитать N значений (здесь expected 1570), но при попытке чтения получил 0 — т.е. данные внезапно закончились.Обычно это признак обрезанного/тронутого файла: обрыв при скачивании, неполная запись, или повреждённый блок данных внутри Parquet (например, повреждённый row-group или отсутствующий footer).Это не нормальная логическая «ошибка в данных» — это физическая ошибка файла.
Возможные причины:
файл не полностью скачан (разрыв соединения во время загрузки);
файл был записан/скопирован с ошибкой (файловая система или диск);
компрессия/кодировка (zstd/snappy) повреждена;
partial write (процесс записи прервался) — footer "PAR1" в конце файла отсутствует.

Имена файлов из логa ошибок:failed_files.txt


3. Анализ файлов top_spot:

// parquet_top_spot_audit.cpp
// Scan top_spot parquet files and detect anomalies.
// Build:
//   g++ -std=gnu++23 -O3 parquet_trade_spot_audit.cpp -lparquet -larrow -lzstd -o parquet_trade_spot_audit
//
// Usage:
//   ./parquet_trade_spot_audit /path/to/parquets output.ndjson [--all]
// Produces NDJSON; by default writes only files that have anomalies. Use --all to emit all files.

Что считается аномалией:
rows_scanned == 0 — файл пустой или не прочитался.
rows_scanned != meta_rows — несоответствие фактического чтения и метаданных.
null_counts > 0 для ts/bid_px/bid_qty/ask_px/ask_qty/valu — пропущенные значения в ключевых столбцах.
non_monotonic_ts > 0 — временные метки уменьшаются (время «откатывается»).
max_gap_ns / gaps_gt_100ms / gaps_gt_1s — большие паузы между соседними TS (аномальные интервалы).
cross_book_count > 0 — строки с bid_px > ask_px (пересечённая книга — критично).
high_fraction_px_zero / high_fraction_qty_zero — слишком большое (по умолчанию >10%) число нулевых цен/количеств.
duplicate_snapshot_count > 0 — повторяющиеся подряд снимки книги (высокая доля подряд идущих одинаковых bid/ask/qty).
meta_rows < MIN (маленький файл, default MIN = 100) — вероятно неполный.
статистические выбросы (опционально): rows_ratio statistical_outlier, px_avg statistical_outlier, qty_avg statistical_outlier, max_gap_ns statistical_outlier — если файл выбивается из распределения по всем файлам (z-score > 3).
