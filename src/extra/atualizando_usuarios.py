from pymongo import MongoClient

def sync_users_with_phrases():
    client = MongoClient("mongodb+srv://laraesquivel:OVyyiX5pIMj4vthh@babys.iuiuuvp.mongodb.net/")
    db = client['babynames']
    users = db['users']
    phrases = db['phrases']

    updated_users = 0

    for user in users.find({}):
        updated_user_phrases = []
        has_update = False

        for user_phrase in user.get('phrases', []):
            frase_texto = user_phrase.get('Frase')
            frase_atualizada = phrases.find_one({'Frase': frase_texto})

            if frase_atualizada:
                # Substitui a frase do usuário pela frase completa da tabela geral
                updated_user_phrases.append(frase_atualizada)
                has_update = True
            else:
                # Caso não encontre, mantém a frase original do usuário
                updated_user_phrases.append(user_phrase)

        if has_update:
            users.update_one(
                {'_id': user['_id']},
                {'$set': {'phrases': updated_user_phrases}}
            )
            updated_users += 1

    print(f"Sincronização concluída. {updated_users} usuários tiveram suas frases atualizadas.")

if __name__ == "__main__":
    sync_users_with_phrases()
