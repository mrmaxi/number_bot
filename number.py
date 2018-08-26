#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Simple Bot to reply to Telegram messages
# This program is dedicated to the public domain under the CC0 license.

from os import environ
from random import randint
from itertools import chain, permutations

from telegram import ReplyKeyboardMarkup
from telegram.ext import (Updater, CommandHandler, MessageHandler, Filters, RegexHandler,
                          ConversationHandler)

from redis_util import RedisDictStore, RedisSimpleStore

import logging

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

logger = logging.getLogger(__name__)


CHOOSING, GUESS_NUMBER, MULTI1, MULTI2, MULTI3 = range(5)

reply_keyboard = [['guess number', 'multi1'],
                  ['multi2', 'multi3']]
markup = ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True)


def start(bot, update):
    update.message.reply_text(
        "Hi! My name is number-bot. I offer you to play several useful games!"
        "Please, choose what game you prefer?",
        reply_markup=markup)

    return CHOOSING


def multi1(bot, update, user_data):
    if user_data.get('choice') == 'multi1':
        ans = update.message.text.lower().strip()
        right_answer = user_data['right_answer']
        question = user_data['question']
        if ans != right_answer:
            update.message.reply_text(f"{ans}? wrong! {question} = {right_answer}")

    m = randint(1, 9)
    n = randint(1, 9)
    mul = m * n
    if randint(0, 1):
        question = f'{m} * {n} = ?'
        right_answer = str(mul)
    else:
        question = f'{mul} : {n} = ?'
        right_answer = str(m)

    user_data.update({
        'choice': 'multi1',
        'question': question,
        'right_answer': right_answer})
    user_data.save()
    update.message.reply_text(question)

    return MULTI1


multiple_table = {}
for n in range(2, 10):
    multiple_table.update({(n, m): n*m for m in range(n, 10)})


multiple_table_r = {}
for q, r in multiple_table.items():
    arr = multiple_table_r.setdefault(r, [])
    arr.append(q)


def multi2(bot, update, user_data):
    if user_data.get('choice') == 'multi2':
        ans = update.message.text.lower().strip()
        q = user_data['q']
        r = user_data['r']
        question = user_data['question']
        right_answer = user_data['right_answer']

        if not ans.replace(' ', '').isdecimal():
            ans = ''.join([c if c.isdecimal() else ' ' for c in ans])

        while '  ' in ans:
            ans = ans.replace('  ', ' ')

        a = list(map(int, ans.split(' ')))
        if not len(a) or len(a) % 2:
            update.message.reply_text(f"{ans}? wrong! {q} = {right_answer}")
        else:
            b = sorted([tuple(sorted([n, m])) for n, m in zip(a[::2], a[1::2])])
            if b != r:
                update.message.reply_text(f"{ans}? wrong! {q} = {right_answer}")

    q, r = list(multiple_table_r.items())[randint(0, len(multiple_table_r) - 1)]
    a = '; '.join(['? x ?'] * len(r))
    question = f"{q} = {a}"
    right_answer = '; '.join(map(lambda n: f'{n[0]} x {n[1]}', multiple_table_r[q]))

    user_data.update({
        'choice': 'multi2',
        'q': q,
        'r': r,
        'question': question,
        'right_answer': right_answer})
    user_data.save()
    update.message.reply_text(question)

    return MULTI2


question_table = []

def my_variants(b, a=()):
    if b:
        for b0 in permutations(b[0], len(b[0])):
            for c in my_variants(b[1:], a + (b0,)):
                yield c
    else:
        yield a


for r, q in multiple_table_r.items():
    question_table.append({
        'question': f'{r}'+'\n= ? x ?'*len(q),
        'right_answer': f'{r}'+''.join(map(lambda a: f'\n= {a[0]} x {a[1]}', q)),
        'answers': list(chain(*[list(my_variants(a)) for a in permutations(q, len(q))]))
    })


def multi3(bot, update, user_data):
    if user_data.get('choice') == 'multi3':
        ans = update.message.text.lower().strip()
        question = user_data['question']
        right_answer = user_data['right_answer']
        answers = user_data['answers']

        def str2tuple(s):
            if not s.replace(' ', '').isdecimal():
                s = ''.join([c if c.isdecimal() else ' ' for c in s])
            while '  ' in s:
                s = s.replace('  ', ' ')
            return tuple(map(int, s.strip().split(' ')))

        a = tuple(map(str2tuple, ans.splitlines()))

        if a and (a in answers or (len(a) == 1 and a[0] in answers)):
            return True
        else:
            logger.info(answers)
            logger.error(a)

            update.message.reply_text(f'{ans}? wrong! {right_answer}')
            return False

    q = question_table[randint(0, len(question_table) - 1)]
    user_data['choice'] = 'multi3'
    user_data.update(q)
    user_data.save()

    update.message.reply_text(q['question'])
    return MULTI3


def calc_nums(num1, num2):
    a = b = 0
    for n, x in enumerate(num1):
        if x in num2:
            a += 1
            if num2[n] == x:
                b += 1
    return (a, b)


def guess_number(bot, update, user_data):
    if user_data.get('choice') == 'guess_number':
        ans = update.message.text.lower().strip()
        right_answer = user_data['right_answer']

        if ans == right_answer:
            user_data.clear()
            user_data.save()
            update.message.reply_text(f'Молодец, угадал!\nя загадал {right_answer}')
            return guess_number(bot, update, user_data)

        if len(set(ans)) != len(ans):
            update.message.reply_text('В моем числе нет повторяющихся цифр, попробуй снова')
            return GUESS_NUMBER

        a, b = calc_nums(ans, right_answer)
        update.message.reply_text(f'{a}:{b}')
        return GUESS_NUMBER

    right_answer = ''
    while len(right_answer) < 4:
        d = str(randint(0, 9))
        if d not in right_answer:
            right_answer += d

    user_data.update({
        'choice': 'guess_number',
        'right_answer': right_answer})
    user_data.save()

    update.message.reply_text("Давай начнем,\nУгадай число что я загадал,\nнапиши число из 4 неповторяющихся цифр, а я подскажу сколько цифр ты угадал, и сколько из них расположил на своем месте.")
    return GUESS_NUMBER


def done(bot, update, user_data):
    choiсe = user_data.get('choice')
    if choiсe in ['multi1', 'multi2', 'multi3']:
        question = user_data['question']
        right_answer = user_data['right_answer']
        update.message.reply_text(f"so, {question}, right answer was {right_answer}\nIt was very nice to play with you")
    elif choiсe == 'guess_number':
        right_answer = user_data['right_answer']
        update.message.reply_text(f"Сдаешься? Я загадал {right_answer}, жаль что не доиграли!")
    else:
        update.message.reply_text("Bye bye!")

    user_data.clear()
    user_data.save()
    return start(bot, update)


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)


def main():
    # Create the Updater and pass it your bot's token.
    token = environ.get('TOKEN')
    redis_url = environ.get('REDIS_URL') or 'redis://redis'
    kwargs = {}

    updater = Updater(token, request_kwargs=kwargs)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    dp.user_data = RedisDictStore(id='user_data', redis_url=redis_url)

    # Add conversation handler with the states GENDER, PHOTO, LOCATION and BIO
    conv_handler = ConversationHandler(
        allow_reentry=True,
        entry_points=[CommandHandler('start', start)],

        states={
            CHOOSING: [RegexHandler('^(guess number)$',
                                    guess_number,
                                    pass_user_data=True),
                       RegexHandler('^(multi1)$',
                                    multi1,
                                    pass_user_data=True),
                       RegexHandler('^(multi2)$',
                                    multi2,
                                    pass_user_data=True),
                       RegexHandler('^(multi3)$',
                                    multi1,
                                    pass_user_data=True),
                       ],

            GUESS_NUMBER: [RegexHandler('[0-9]{4}',
                                    guess_number,
                                    pass_user_data=True),
                    ],

            MULTI1: [RegexHandler('([0-9]|[ ])+',
                                    multi1,
                                    pass_user_data=True),
                    ],

            MULTI2: [RegexHandler('([0-9]|[ ])+',
                                    multi2,
                                    pass_user_data=True),
                     ],

            MULTI3: [RegexHandler('([0-9]|[ ])+',
                                    multi3,
                                    pass_user_data=True),
                     ],
        },

        fallbacks=[RegexHandler('^Done$', done, pass_user_data=True)]
    )
    conv_handler.conversations = RedisSimpleStore(id='main_conversations', redis_url=redis_url)
    dp.add_handler(conv_handler)

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()