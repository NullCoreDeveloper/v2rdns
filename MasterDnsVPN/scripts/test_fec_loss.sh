#!/bin/bash

# Скрипт для тестирования Adaptive FEC в MasterDnsVPN
# Использует tc (traffic control) для имитации потерь пакетов на loopback интерфейсе (lo).

IFACE="lo"
PORT=53
LOSS_RATE=${1:-"20%"}

echo "=== Adaptive FEC Test Environment ==="

# Проверка прав root
if [ "$EUID" -ne 0 ]; then
  echo "Пожалуйста, запустите скрипт с правами root (sudo)."
  exit 1
fi

start_loss() {
    echo "Включаем потерю пакетов: $LOSS_RATE на интерфейсе $IFACE (порт $PORT)"
    
    # Очистка предыдущих правил
    tc qdisc del dev $IFACE root 2>/dev/null
    
    # Создаем корневое правило (HTB для фильтрации)
    tc qdisc add dev $IFACE root handle 1: htb default 10
    
    # Класс для всего остального трафика (без потерь)
    tc class add dev $IFACE parent 1: classid 1:10 htb rate 1000mbit
    
    # Класс для трафика MasterDnsVPN (с потерями)
    tc class add dev $IFACE parent 1: classid 1:20 htb rate 1000mbit
    tc qdisc add dev $IFACE parent 1:20 handle 20: netem loss $LOSS_RATE
    
    # Направляем UDP трафик на порту 53 в класс с потерями
    tc filter add dev $IFACE protocol ip parent 1: prio 1 u32 match ip dport $PORT 0xffff flowid 1:20
    tc filter add dev $IFACE protocol ip parent 1: prio 1 u32 match ip sport $PORT 0xffff flowid 1:20
    
    echo "Готово! Трафик на UDP/$PORT теперь теряется с вероятностью $LOSS_RATE."
    echo "Запустите сервер и клиент MasterDnsVPN локально, чтобы проверить адаптацию FEC."
}

stop_loss() {
    echo "Отключаем потери пакетов на интерфейсе $IFACE..."
    tc qdisc del dev $IFACE root 2>/dev/null
    echo "Готово. Сеть восстановлена."
}

status() {
    echo "Текущие правила qdisc на $IFACE:"
    tc qdisc show dev $IFACE
    echo "Текущие фильтры на $IFACE:"
    tc filter show dev $IFACE
}

case "$1" in
    start)
        LOSS_RATE=${2:-"20%"}
        start_loss
        ;;
    stop)
        stop_loss
        ;;
    status)
        status
        ;;
    *)
        echo "Использование: $0 {start [loss_rate]|stop|status}"
        echo "Пример: $0 start 25%"
        exit 1
esac

exit 0
