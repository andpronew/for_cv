
g++ -std=gnu++23 -O3 parquet_audit_new.cpp -lparquet -larrow -lzstd -o parquet_audit_new

./parquet_audit_new /home/andpro/Documents/parquet_reader/parquet_files_top_spot/*.parquet --out=parquet_audit_report_top.txt
.........................................
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_top_spot/bn_top_spot_DFUSDT_2025_9_5.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_top_spot/bn_top_spot_DFUSDT_2025_9_6.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_top_spot/bn_top_spot_DFUSDT_2025_9_7.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_top_spot/bn_top_spot_DFUSDT_2025_9_8.parquet
No problematic files found (report written to parquet_audit_report_top.txt).
Parquet audit report
====================
No problematic files found.
End of report

./parquet_audit_new /home/andpro/Documents/parquet_reader/parquet_files_depth_spot/*.parquet --out=parquet_audit_report_depth.txt
........................................
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_depth_spot/bn_depth_spot_DFUSDT_2025_9_5.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_depth_spot/bn_depth_spot_DFUSDT_2025_9_6.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_depth_spot/bn_depth_spot_DFUSDT_2025_9_7.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_depth_spot/bn_depth_spot_DFUSDT_2025_9_8.parquet
Wrote audit report to: parquet_audit_report_depth.txt (problematic files: 309)

./parquet_audit_new /home/andpro/Documents/parquet_reader/parquet_files_trade_spots/*.parquet --out=parquet_audit_report_trade.txt
..................................
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_trade_spots/bn_trade_spot_DFUSDT_2025_9_5.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_trade_spots/bn_trade_spot_DFUSDT_2025_9_6.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_trade_spots/bn_trade_spot_DFUSDT_2025_9_7.parquet
Auditing: /home/andpro/Documents/parquet_reader/parquet_files_trade_spots/bn_trade_spot_DFUSDT_2025_9_8.parquet
No problematic files found (report written to parquet_audit_report_trade.txt).
Parquet audit report
====================
No problematic files found.
End of report

parquet_audit_new.cpp - аудитор Parquet-файлов, то есть утилита для проверки корректности и целостности данных.
Она не парсит данные глубоко (как parquet_reader.cpp, который выводит содержимое), а пробегает по всем строкам файла и проверяет:
- нет ли битых строк (ошибки чтения);
- корректность типов столбцов;
- не нарушена ли сортировка по времени;
- нет ли "дыр" в данных (например, время прыгает назад);
- возможно, дубликаты записей или некорректные null-значения.

Итог — короткий текстовый отчёт по каждому файлу.
Определяет тип файла по имени (top|trade|depth).
Ищет колонки: ts, firstId, lastId, ask_px, ask_qty, bid_px, bid_qty, а также возможные offset-колонки типа ask_off, ask_offset, ask_offsets, bid_off и т.п.
Поддерживает два формата для массивов в parquet:
flattened arrays + explicit offsets (ask_off/bid_off) — читаем и сверяем;
flattened arrays без offsets — помечаем flattened_without_offsets (информационно).

Делает подсчёты: non_monotonic_ts, lastId < firstId, id_overlap, id_gap, zero-price fractions, zero-qty fractions, per-row offsets mismatches, crossed-book (best_bid >= best_ask), price-change >10× между соседними рядами (для px-колонок, если есть), экстремальные отклонения qty (>10^6 × от среднего), проверка кратности (price % 1000 == 0, qty % 1e8 == 0).

Записывает подробные статистики и краткий summary для каждого файла в результирующий txt.

1. Общие замечания по интерпретации:
«Строка» / «row» — это запись в таблице (элемент per-row, не элемент вложенного массива).
«Элемент» — элемент flattened-массива (например, все ask_px элементы).
Многие проверки требуюt наличия offset-колонок (например, ask_off, bid_off) чтобы корректно маппить flattened-массивы на строки. Если offset-колонки отсутствуют, flattened_arrays_without_offsets выставляется как информационный флаг и подробные пер-строковые проверки по массивам пропускаются/ограничены.
По умолчанию утилита помечает файл проблемным, если обнаружено любое из реальных несоответствий. Информационные предупреждения игнорируются, если не указан флаг --include-info.

2. Полный список критериев (состояние реализации):
A. Таймстэмпы и порядок
non_monotonic_ts
Что: обнаружение строк, где ts (наносекунды) становится меньше, чем в предыдущей строке.
Как: проход по per-row ts; если ts[i] < ts[i-1] → increment.
Тип: ошибка/важное предупреждение.
Отчёт: поле non_monotonic_ts (count).

B. ID-диапазоны (для delta/depth rows)
lastId < firstId
Что: внутри одной строки lastId меньше firstId.
Как: если per-row firstId и lastId присутствуют и lastId < firstId → increment.
Тип: ошибка.
Отчёт: lastId < firstId.

id_overlap_count
Что: перекрытие диапазонов соседних строк: firstId <= prev.lastId.
Как: если есть prev.lastId и firstId <= prev.lastId → increment.
Тип: предупреждение/ошибка (возможная дубликация или неправильная агрегация).
Отчёт: id_overlap_count.

id_gap_count
Что: пропуск ID: firstId > prev.lastId + 1.
Как: проверка соседних строк.
Тип: предупреждение (потеря сегментов).
Отчёт: id_gap_count.

C. Структура массивов / offsets
flattened_arrays_without_offsets
Что: в файле есть flattened элементы (ask_px/bid_px и т.п.), но отсутствуют offset-колонки (ask_off/bid_off), поэтому нельзя корректно сопоставить элементы строкам.
Тип: информационный (ограничение анализа).
Отчёт: flattened_arrays_without_offsets (yes/no).

per_row_offsets_mismatch
Что: присутствуют offset-колонки, но их длина (в RG) меньше, чем rows_in_rg + 1 — явная несогласованность.
Тип: информационный / предупреждение.
Отчёт: per_row_offsets_mismatch.

D. Нулевые/пустые значения и согласованность массивов
has_bid_px but bid_px_count == 0 (и аналогично для ask)
Что: колонка объявлена (существует), но число элементарных значений в массиве равно нулю — подозрительно.
Тип: предупреждение/ошибка (в зависимости от контекста).
Реализовано: да (текущее поведение: если объявлена колонка, но *_px_count==0, в отчёте это поле заполнено значением rows_scanned).
Отчёт: has_bid_px_but_zero_count, has_ask_px_but_zero_count.

per-element zero counts (bid_qty_zero / ask_qty_zero / bid_px_zero / ask_px_zero)
Что: число элементарных значений, равных 0, в flattened массивах.
Как: считается по элементам массивов во всём файле.
Тип: предупреждение, если доля высока. Текущее пороговое значение для пометки «высокой доли» — не реализовано непосредственно; утилита возвращает абсолютные счётчики, а долю можно вычислить вручную (элементов / rows).
Отчёт: bid_qty_zero, ask_qty_zero, bid_px_zero, ask_px_zero.

per-row bid/ask counts mismatch (rows_scanned vs offsets or array lengths)
Что: если объявлены per-row offsets, но их размеры не соответствуют rows + 1 или суммарное число элементов не согласуется с offset-диапазонами.
Тип: предупреждение/инфо.

E. Логика книги (best bid/ask)
crossed_book_count (best_bid >= best_ask)
Что: на строке лучшая заявка на продажу (ask) равна или меньше лучшей заявки на покупку (bid) — книжная аномалия (crossed book).
Как: при наличии first_bid_px и first_ask_px (из offsets) проверяется bid >= ask.
Тип: предупреждение/ошибка (в зависимости от контекста — обычно ошибка).
Отчёт: crossed_book_count.

первый bid < первый ask в одной строке
Что: пользователь просил контролировать порядок; текущая проверка реализована обратной проверкой (crossed_book) — если bid >= ask это ошибка.

F. Динамика цены и объёма
price_change_10x_count
Что: цена между смежными строками изменилась более, чем в 10×.
Как: берётся репрезентативная цена строки (первый ask или первый bid; fallback — per-row scalar если есть). Если max(cur,prev)/min(cur,prev) > 10 → increment.
Тип: предупреждение.
Отчёт: price_change_10x_count.

qty_extreme_deviation_count («количество отличалось от среднего более чем в 1e6×»)
Что: пользователь желал детекцию, если per-row qty отличается от среднего больше чем в 1 000 000 раз.
Как: требовало бы сбора всех per-row qty и подсчёта фракции с отклонением >1e6×.
Реализовано: нет (в текущей версии не хранится весь массив per-row qty из-за возможной памяти). В коде есть аккумулирующие счётчики (sum_qty, qty_samples), но без хранения распределения нельзя точно определить долю экстремалов.
Предложение: можно реализовать двумя способами:
a) один проход: вычислить среднее, второй проход: подсчитать долю экстремалов (требует двойного чтения RG или буферизации);
b) поддерживать streaming-статистики (например, детектор выбросов на основе медианы/quantiles) — потребуется память/время.

G. Формат единиц и кратность
price_not_div1000_count
Что: цена (в минимальных единицах, сатоши для примера) не делится на 1000. Ожидание: цена кратна 1000.
Как: проверяется px % 1000 != 0 на доступных per-row первых значениях.
Тип: предупреждение (несоответствие формату).
Отчёт: price_not_div1000_count.

qty_not_div1e8_count
Что: объём (в минимальных единицах) не делится на 1e8 (например, сатоши).
Как: проверка qty % 100000000 != 0.
Тип: предупреждение/формат.
Отчёт: qty_not_div1e8_count.

3. Какая информация появляется в итоговом отчёте
Для каждого файла выводятся (существующие поля в отчёте):
file_path, type, rows_scanned
счётчики: non_monotonic_ts, lastId < firstId, id_overlap_count, id_gap_count
элементы массивов: ask_px_count, bid_px_count, ask_qty_count, bid_qty_count
has_bid_px_but_zero_count, has_ask_px_but_zero_count
элементы с нулями: bid_qty_zero, ask_qty_zero, bid_px_zero, ask_px_zero
crossed_book_count, price_change_10x_count, price_not_div1000_count, qty_not_div1e8_count
информационные флаги: flattened_arrays_without_offsets, per_row_offsets_mismatch
подробные поясняющие сообщения (текстовые рекомендации/объяснения) — только для проблемных файлов (если вы используете фильтрацию --include-info, то информационные флаги также попадут в отчёт).

4. Что считается «проблемой» (по умолчанию)
Файл считается проблемным и попадает в финальный report.txt, если присутствует любая из следующих условий:
non_monotonic_ts > 0
lastId < firstId > 0
id_overlap_count > 0
id_gap_count > 0
has_bid_px_but_zero_count > 0 или has_ask_px_but_zero_count > 0
bid_qty_zero > 0 или ask_qty_zero > 0
bid_px_zero > 0 или ask_px_zero > 0
crossed_book_count > 0
price_change_10x_count > 0
price_not_div1000_count > 0
qty_not_div1e8_count > 0
Информационные флаги (flattened_arrays_without_offsets, per_row_offsets_mismatch) по умолчанию не делают файл "проблемным", но при запуске с --include-info они также будут трактоваться как проблемы.

5. Ограничения текущей реализации:
Для точной per-row валидации flattened массивов (например, подсчёт доли строк с bid_qty==0) требуется либо наличие offset-колонок, либо хранение/распрямление всех элементов в память. Сейчас:
если offsets есть — часть пер-строковых проверок выполняется корректно (берётся первый элемент строки и т.д.);
если offsets отсутствуют — утилита помечает это как информационное ограничение и не делает детальной per-row проверки.
Для проверки долей (например, «fraction of zeros > 10%») утилита возвращает абсолютные счётчики; доли можно вычислить вручную как count_zero / total_elements. Автоматический триггер по порогу 10% не реализован (но легко добавить).
Проверка экстремального отклонения qty ( >1e6× от среднего) не реализована в одном проходе — нужна либо двойная проходка, либо буферизация.

6. Как интерпретировать результаты на практике:
Если non_monotonic_ts > 0 — высокий приоритет: нарушен временной порядок, нужно проверить источник агрегатора/скрипт сборщика.
Если id_overlap_count > 0 — возможна дубликация при загрузке/скачивании.
ask_px_zero / bid_px_zero > 0 — почти всегда ошибка записи/агрегатора: цена не должна быть 0.
crossed_book_count > 0 — проверьте, не поменялся ли смысл колонок (ask/bid swapped) или не случился баг в агрегаторе.
flattened_arrays_without_offsets — не обязательно ошибка данных, но мешает детальному аудиту; лучше производить файлы с offset-колонками.
