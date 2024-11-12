from airflow.providers.telegram.hooks.telegram import TelegramHook 

def send_telegram_success_message(context): 
    hook = TelegramHook(telegram_conn_id='test',
                        token='7939310969:AAEJdfH2s9mpOA8GaobNsRuHOytatAzNkys',
                        chat_id='-4501640144')
    dag = context['task_instance_key_str']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' 
    hook.send_message({
        'chat_id': '-4501640144',
        'text': message
    }) 


def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='7939310969:AAEJdfH2s9mpOA8GaobNsRuHOytatAzNkys',
                        chat_id='-4501640144')

    run_id = context['run_id']
    dag = context['task_instance_key_str']
    message = f'Исполнение DAG {dag} с id={run_id} прошло неуспешно!' 
    hook.send_message({
        'chat_id': '-4501640144',
        'text': message
    })