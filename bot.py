# 1. Коли користувач натискає "Скасувати запис"
if text == "Скасувати запис ❌":
    data = last_rec.get(chat_id, {})
    if not data:
        await msg.reply_text("У вас немає активних записів. 📭", reply_markup=main_kb)
        return
    
    reply_text = "Ваші записи:\n\n"
    for i, (event_id, record) in enumerate(data.items(), 1):
        reply_text += (
            f"{i}. <b>ID запису:</b> <code>{record['record_code']}</code>\n"
            f"   📅 <b>Дата і час:</b> {record['full_dt']}\n\n"
        )
    
    reply_text += "Надішли тільки <b>ID запису</b> (наприклад, <code>REC-20251117-1300</code>), щоб скасувати:"
    await msg.reply_text(reply_text, reply_markup=cancel_kb, parse_mode="HTML")
    return


# 2. Коли користувач натискає "Список записів"
if text == "Список записів 📋":
    data = last_rec.get(chat_id, {})
    if not data:
        await msg.reply_text("У вас немає активних записів. 📭", reply_markup=main_kb)
        return
    
    reply_text = "Ваші записи:\n\n"
    for i, (event_id, record) in enumerate(data.items(), 1):
        reply_text += (
            f"{i}. <b>ID запису:</b> <code>{record['record_code']}</code>\n"
            f"   📅 <b>Дата і час:</b> {record['full_dt']}\n\n"
        )
    
    await msg.reply_text(reply_text, reply_markup=main_kb, parse_mode="HTML")
    return
