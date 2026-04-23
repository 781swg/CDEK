import pandas as pd

# Читаем исходники
print("Грузим данные...")
df_doc = pd.read_csv('document.csv', parse_dates=['date_created', 'date_closed', 'tab_changed'])
df_ops = pd.read_csv('operation_history.csv', parse_dates=['date_created', 'tab_changed'])

# --- ЗАДАЧА 1: Ищем первый документ на конечном складе ---
print("Решаем Задачу 1...")

# Сортируем операции по времени для восстановления хронологии
df_ops_sorted = df_ops.sort_values(by=['cargo_place_uuid', 'date_created']).copy()

# Отлавливаем моменты, когда груз переезжает на новый склад
df_ops_sorted['office_changed'] = df_ops_sorted.groupby('cargo_place_uuid')['current_office_uuid'].shift() != df_ops_sorted['current_office_uuid']

# Нумеруем "заезды" на склады, чтобы правильно обработать транзиты
df_ops_sorted['block_id'] = df_ops_sorted.groupby('cargo_place_uuid')['office_changed'].cumsum()

# Ищем последний (целевой) склад для каждой посылки
last_blocks = df_ops_sorted.groupby('cargo_place_uuid')['block_id'].max().reset_index()

# Отсекаем всё транзитное, оставляем только операции на конечном складе
final_office_ops = df_ops_sorted.merge(last_blocks, on=['cargo_place_uuid', 'block_id'])

# Вытаскиваем самую первую операцию после прибытия на этот склад
first_op_at_dest = final_office_ops.groupby('cargo_place_uuid').first().reset_index()

# Джоиним с таблицей документов, чтобы достать их реальные номера
result_task1 = first_op_at_dest[['cargo_place_uuid', 'document_uuid']].merge(
    df_doc[['uuid', 'number']],
    left_on='document_uuid',
    right_on='uuid',
    how='left'
)

# Причесываем таблицу и сохраняем ответ
result_task1 = result_task1[['cargo_place_uuid', 'number']].rename(columns={'number': 'first_document_number'})
result_task1.to_csv('Кухтин_Алексей_1.csv', index=False)
print("Готово! Ответ в 'Кухтин_Алексей_1.csv'\n")


# --- ЗАДАЧА 2: Считаем метрики ---
print("Считаем метрики для Задачи 2...\n")

# 1. Lead Time: сколько в среднем часов обрабатывается один груз
lead_times = df_ops.groupby('cargo_place_uuid')['date_created'].agg(['min', 'max'])
lead_times['lead_time_hours'] = (lead_times['max'] - lead_times['min']).dt.total_seconds() / 3600
print(f"1. Средний Lead Time грузоместа: {lead_times['lead_time_hours'].mean():.2f} часов")

# 2. Среднее число операций на одну посылку
ops_per_cargo = df_ops.groupby('cargo_place_uuid').size()
print(f"2. Операций на посылку: {ops_per_cargo.mean():.2f} шт.")

# 3. Как быстро закрывают документы (откидываем незакрытые)
df_doc_closed = df_doc.dropna(subset=['date_closed']).copy()
df_doc_closed['closure_time_hours'] = (df_doc_closed['date_closed'] - df_doc_closed['date_created']).dt.total_seconds() / 3600
print(f"3. Среднее время закрытия документа: {df_doc_closed['closure_time_hours'].mean():.2f} часов")

# 4. Процент проблемных документов (возвраты и корректировки)
problem_ops = ['RETURN_UNDELIVERED', 'CORRECTION_INCOME']
problem_ratio = (df_ops['document_type'].isin(problem_ops).sum() / len(df_ops)) * 100
print(f"4. Доля проблемных статусов: {problem_ratio:.2f}%")