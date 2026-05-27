#!/usr/bin/env python3
import sys
import json
import base64
import urllib.parse

def main():
    if len(sys.argv) < 2:
        print("Использование: python3 encode_mdns.py <путь_к_файлу.json> [название_профиля]")
        print("Или передайте JSON через конвейер (stdin): cat config.json | python3 encode_mdns.py - [название_профиля]")
        sys.exit(1)

    filepath = sys.argv[1]
    name = sys.argv[2] if len(sys.argv) > 2 else "MasterDNSVPN"

    try:
        if filepath == "-":
            json_content = sys.stdin.read()
        else:
            with open(filepath, 'r', encoding='utf-8') as f:
                json_content = f.read()

        # Парсим JSON для валидации и сжатия (убираем лишние пробелы и переносы)
        data = json.loads(json_content)
        compact_json = json.dumps(data, separators=(',', ':'))

        # Кодируем в Base64
        base64_bytes = base64.b64encode(compact_json.encode('utf-8'))
        base64_str = base64_bytes.decode('utf-8')

        # Кодируем название профиля для URL
        url_encoded_name = urllib.parse.quote(name)

        # Выводим готовую ссылку
        print(f"mdns://{base64_str}#{url_encoded_name}")

    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
