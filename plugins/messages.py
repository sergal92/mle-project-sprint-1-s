from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context):
    """Отправка сообщения в Telegram при успешном исполнении DAG"""
    hook = TelegramHook(token='sergal92_bot', chat_id='5280585110')
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'✅ Исполнение DAG {dag_id} с id={run_id} завершилось УСПЕШНО!'
    
    hook.send_message({
        'chat_id': '5280585110',
        'text': message
    })

def send_telegram_failure_message(context):
    """Отправка сообщения в Telegram при неудачном исполнении DAG"""
    hook = TelegramHook(token='sergal92_bot', chat_id='5280585110')
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context.get('task_instance_key_str', 'Unknown')
    
    message = f'❌ Исполнение DAG {dag_id} с id={run_id} и task_instance_key_str={task_instance_key_str} завершилось НЕУДАЧЕЙ!'
    
    hook.send_message({
        'chat_id': '5280585110',
        'text': message
    })