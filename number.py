#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Simple Bot to reply to Telegram messages
# This program is dedicated to the public domain under the CC0 license.

from os import environ
from random import randint, normalvariate
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


CHOOSING, GUESS_NUMBER, MULTI1, MULTI2, MULTI3, TWO_ACTIONS, RANDOM = range(7)

reply_keyboard = [
    ['guess number', 'multi1'],
    ['multi2', 'two_actions'],
    ['random']
]

markup = ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=True)
in_game_markup = ReplyKeyboardMarkup([['Done']], resize_keyboard=True)

def start(bot, update, user_data):
    user_data.clear()
    user_data.save()

    update.message.reply_text(
        "Hi! My name is number-bot. I offer you to play several useful games!"
        "Please, choose what game you prefer?",
        reply_markup=markup)

    return CHOOSING


def new_multi1():
    """ Примеры на таблицу умножения.
        - Умножение одного числа на другое
        - Деление чисел из таблицы на один из множителей
    """

    m = randint(1, 9)
    n = randint(1, 9)
    mul = m * n
    if randint(0, 1):
        question = f'{m} * {n} = ?'
        right_answer = str(mul)
    else:
        question = f'{mul} : {n} = ?'
        right_answer = str(m)

    return {
        'quest_type': 'multi1',
        'question': question,
        'right_answer': right_answer
    }


def test_simple(user_data, ans):
    right_answer = user_data['right_answer']
    question = user_data['question']
    if ans != right_answer:
        return f"{ans}? wrong! {question} = {right_answer}"
    return None


multiple_table = {}
for n in range(2, 10):
    multiple_table.update({(n, m): n*m for m in range(n, 10)})


multiple_table_r = {}
for q, r in multiple_table.items():
    arr = multiple_table_r.setdefault(r, [])
    arr.append(q)


def new_multi2():
    """ Примеры на таблицу умножения в обратную сторону
        Необходимо угадать что на что надо умножить, чтобы получилось заданное число из таблицы умножения
        возможны несколько вариантов ответа, необходимо назвать их все
    """

    q, r = list(multiple_table_r.items())[randint(0, len(multiple_table_r) - 1)]
    a = '; '.join(['? x ?'] * len(r))
    question = f"{q} = {a}"
    right_answer = '; '.join(map(lambda n: f'{n[0]} x {n[1]}', multiple_table_r[q]))

    return {
        'quest_type': 'multi2',
        'q': q,
        'r': r,
        'question': question,
        'right_answer': right_answer
    }


def test_multi2(user_data, ans):
    q = user_data['q']
    r = list(map(tuple, user_data['r']))
    right_answer = user_data['right_answer']

    if not ans.replace(' ', '').isdecimal():
        ans = ''.join([c if c.isdecimal() else ' ' for c in ans])

    while '  ' in ans:
        ans = ans.replace('  ', ' ')

    a = list(map(int, ans.split(' ')))
    if not len(a) or len(a) % 2:
        return f"{ans}? wrong! {q} = {right_answer}"
    else:
        b = sorted([tuple(sorted([n, m])) for n, m in zip(a[::2], a[1::2])])
        if b != r:
            return f"{ans}? wrong! {q} = {right_answer}"

    return None


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
        'quest_type': 'multi3',
        'question': f'{r}'+'\n= ? x ?'*len(q),
        'right_answer': f'{r}'+''.join(map(lambda a: f'\n= {a[0]} x {a[1]}', q)),
        'answers': list(set(chain(*[list(my_variants(a)) for a in permutations(q, len(q))])))
    })


def new_multi3():
    return question_table[randint(0, len(question_table) - 1)]


def str2tuple(s):
    if not s.replace(' ', '').isdecimal():
        s = ''.join([c if c.isdecimal() else ' ' for c in s])
    while '  ' in s:
        s = s.replace('  ', ' ')
    return tuple(map(int, s.strip().split(' ')))


def test_multi3(user_data, ans):
    right_answer = user_data['right_answer']
    answers = user_data['answers']

    a = tuple(map(str2tuple, ans.splitlines()))

    if a and (a in answers or (len(a) == 1 and a[0] in answers)):
        return None
    else:
        logger.info(answers)
        logger.error(a)
        return f'{ans}? wrong! {right_answer}'


def new_two_actions():
    """ Примеры в два действия по сложению и умножению
         a + b * c
         a - b * c
         a + b : c
         a - b : c
        (a + b) * c
        (a - b) * c
        (a + b) : c
        (a - b) : c
    """

    quest_type = 'two_actions'

    v = randint(0, 7)
    if v == 0:
        b = round(abs(normalvariate(0, 30)))
        c = round(abs(normalvariate(0, 10)))
        a = round(abs(normalvariate(0, 300)))
        return {
            'quest_type': quest_type,
            'question': f'{a} + {b} * {c}',
            'right_answer': str(a + b * c)
        }
    elif v == 1:
        b = 1 + round(abs(normalvariate(0, 30)))
        c = randint(1, 10)
        a = b*c + round(abs(normalvariate(0, 300)))
        return {
            'quest_type': quest_type,
            'question': f'{a} - {b} * {c}',
            'right_answer': str(a - b * c)
        }
    elif v == 2:
        b_c = 1 + round(abs(normalvariate(0, 10)))
        a = round(abs(normalvariate(0, 30)))
        c = 1+ round(abs(normalvariate(0, 30)))
        b = b_c * c

        return {
            'quest_type': quest_type,
            'question': f'{a} + {b} : {c}',
            'right_answer': str(a + b_c)
        }
    elif v == 3:
        b_c = 1 + round(abs(normalvariate(0, 10)))
        a = b_c + round(abs(normalvariate(0, 30)))
        c = 1 + round(abs(normalvariate(0, 10)))
        b = b_c * c

        return {
            'quest_type': quest_type,
            'question': f'{a} - {b} : {c}',
            'right_answer': str(a - b_c)
        }
    if v == 4:
        a = round(abs(normalvariate(0, 30)))
        b = round(abs(normalvariate(0, 10)))
        c = round(abs(normalvariate(0, 30)))
        return {
            'quest_type': quest_type,
            'question': f'({a} + {b}) * {c}',
            'right_answer': str((a + b) * c)
        }
    elif v == 5:
        b = round(abs(normalvariate(0, 30)))
        ab = 1 + round(abs(normalvariate(0, 30)))
        a = b + ab
        c = round(abs(normalvariate(0, 10)))
        return {
            'quest_type': quest_type,
            'question': f'({a} - {b}) * {c}',
            'right_answer': str((a - b) * c)
        }
    elif v == 6:
        ab_c = 1+round(abs(normalvariate(0, 10)))
        c = 1 + round(abs(normalvariate(0, 10)))
        ab = ab_c * c
        a = randint(1, ab)
        b = ab - a

        return {
            'quest_type': quest_type,
            'question': f'({a} + {b}) : {c}',
            'right_answer': str(ab_c)
        }
    elif v == 7:
        ab_c = 1 + round(abs(normalvariate(0, 10)))
        c = 1 + round(abs(normalvariate(0, 10)))
        ab = ab_c * c
        a = ab + round(abs(normalvariate(0, 30)))
        b = a - ab

        return {
            'quest_type': quest_type,
            'question': f'({a} - {b}) : {c}',
            'right_answer': str(ab_c)
        }


def test_answer(update, user_data, quest_type, test_func, rules=None):
    if user_data.get('quest_type', user_data.get('choice')) == quest_type:
        r = test_func(user_data, update.message.text.lower().strip())
        if r:
            update.message.reply_text(r)
    elif rules:
        update.message.reply_text(rules)


def ask_question(update, user_data, choice_name, new_quest, ret):
    user_data['choice'] = choice_name
    user_data.update(new_quest)
    user_data.save()
    update.message.reply_text(new_quest['question'])#, reply_markup=in_game_markup)
    return ret


def multi1(bot, update, user_data):
    test_answer(update, user_data, 'multi1', test_simple, new_multi1.__doc__)

    new_quest = new_multi1()
    return ask_question(update, user_data, 'multi1', new_quest, MULTI1)


def multi2(bot, update, user_data):
    test_answer(update, user_data, 'multi2', test_multi2, new_multi2.__doc__)

    new_quest = new_multi2()
    return ask_question(update, user_data, 'multi2', new_quest, MULTI2)


def multi3(bot, update, user_data):
    test_answer(update, user_data, 'multi3', test_multi3, new_multi3.__doc__)

    new_quest = new_multi3()
    return ask_question(update, user_data, 'multi3', new_quest, MULTI3)


def two_actions(bot, update, user_data):
    test_answer(update, user_data, 'two_actions', test_simple, new_multi3.__doc__)

    new_quest = new_two_actions()
    return ask_question(update, user_data, 'two_actions', new_quest, TWO_ACTIONS)


games = {
        'multi1': (new_multi1, test_simple),
        'multi2': (new_multi2, test_multi2),
        'two_actions': (new_two_actions, test_simple),
    }


def random(bot, update, user_data):
    """произвольный пример из multi"""

    quest_type = user_data.get('quest_type', user_data.get('choice'))
    if quest_type in games:
        game = games[quest_type]
        test_answer(update, user_data, quest_type, game[1])
    else:
        update.message.reply_text('Различные примеры на умножение и деление из multi1, multi2, two_actions')

    quest_type = list(games.keys())[randint(0, len(games)-1)]
    new_quest = games[quest_type][0]()
    return ask_question(update, user_data, quest_type, new_quest, RANDOM)


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
            update.message.reply_text('В моем числе нет повторяющихся цифр, попробуй снова')#, reply_markup=in_game_markup)
            return GUESS_NUMBER

        a, b = calc_nums(ans, right_answer)
        update.message.reply_text(f'{a}:{b}')#, reply_markup=in_game_markup)
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

    update.message.reply_text("Давай начнем,\nУгадай число что я загадал,\nнапиши число из 4 неповторяющихся цифр, а я подскажу сколько цифр ты угадал, и сколько из них расположил на своем месте.")#, reply_markup=in_game_markup)
    return GUESS_NUMBER


def done(bot, update, user_data):
    choiсe = user_data.get('choice')
    if choiсe in ['multi1', 'multi2', 'multi3', 'two_actions', 'random']:
        question = user_data['question']
        right_answer = user_data['right_answer']
        update.message.reply_text(f"so, {question}, right answer was {right_answer}\nIt was very nice to play with you")
    elif choiсe == 'guess_number':
        right_answer = user_data['right_answer']
        update.message.reply_text(f"Сдаешься? Я загадал {right_answer}, жаль что не доиграли!")
    else:
        update.message.reply_text("Bye bye!")

    return start(bot, update, user_data)


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)


def main():
    # Create the Updater and pass it your bot's token.
    token = environ.get('TOKEN')
    redis_url = environ.get('REDIS_URL') or 'redis://redis'
    kwargs = {}

    updater = Updater(token, request_kwargs=kwargs, use_context=False)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    dp.user_data = RedisDictStore(id='user_data', redis_url=redis_url)

    # Add conversation handler with the states GENDER, PHOTO, LOCATION and BIO
    conv_handler = ConversationHandler(
        allow_reentry=True,
        entry_points=[CommandHandler('start', start, pass_user_data=True)],

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
                                    multi3,
                                    pass_user_data=True),
                       RegexHandler('^(two_actions)$',
                                    two_actions,
                                    pass_user_data=True),
                       RegexHandler('^(random)$',
                                    random,
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

            TWO_ACTIONS: [RegexHandler('([0-9]|[ ])+',
                                  two_actions,
                                  pass_user_data=True),
                     ],

            RANDOM: [RegexHandler('([0-9]|[ ])+',
                                  random,
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