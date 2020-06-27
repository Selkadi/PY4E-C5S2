import sqlite3

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

# Find the ids that send out page rank - we only are interested
# in pages in the SCC that have in and out links
cur.execute('''SELECT DISTINCT from_id FROM Links''')
from_ids = list()
for row in cur:
    from_ids.append(row[0])

# Find the ids that receive page rank; monta lista de-para
to_ids = list()
links = list()
cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id : continue
    if from_id not in from_ids : continue
    if to_id not in from_ids : continue
    # print('1.',from_id, to_id)
    links.append(row)
    if to_id not in to_ids : to_ids.append(to_id)


# Get latest page ranks for strongly connected component; ontem o rank anterior
prev_ranks = dict()
for node in from_ids:
    cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (node, ))
    row = cur.fetchone()
    prev_ranks[node] = row[0]

sval = input('How many iterations:')
many = 1
if ( len(sval) > 0 ) : many = int(sval)

# Sanity check
if len(prev_ranks) < 1 :
    # print("Nothing to page rank.  Check data.")
    quit()

# Lets do Page Rank in memory so it is really fast
for i in range(many):
    # print prev_ranks.items()[:5]
    next_ranks = dict();
    total = 0.0

    # acumula ranking anterior das páginas (de)
    for (node, old_rank) in list(prev_ranks.items()):
        total = total + old_rank
        next_ranks[node] = 0.0
    # print('2.',i, 'total anterior:', total)

    # Find the number of outbound links and sent the page rank down each
    # Calcula a qtde de "paras" para cada "de"
    for (node, old_rank) in list(prev_ranks.items()):
        # print node, old_rank
        give_ids = list()
        for (from_id, to_id) in links:
            if from_id != node : continue
           #  print '   ',from_id,to_id

            if to_id not in to_ids: continue
            give_ids.append(to_id)
        if ( len(give_ids) < 1 ) : continue
        amount = old_rank / len(give_ids)
        # print('3.',node, old_rank, amount, len(give_ids))

        # distribui a média do ranking anterior (de) para cada página (para)
        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount
            # print('4.',id, next_ranks[id])

    # soma os novos rankings (de)
    newtot = 0
    for (node, next_rank) in list(next_ranks.items()):
        # print('5.',node, next_rank)
        newtot = newtot + next_rank

    # distribui a média da diferença entre totais ranking novo e atual para cada página
    evap = (total - newtot) / len(next_ranks)
    # print('6.',total, newtot, len(next_ranks), evap)

    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap
        # print('7.', next_ranks[node])

    #soma os rankings após a distribuição
    newtot = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank
    # print('8.',newtot)

    # Compute the per-page average change from old rank to new rank
    # As indication of convergence of the algorithm
    totdiff = 0
    for (node, old_rank) in list(prev_ranks.items()):
        new_rank = next_ranks[node]
        diff = abs(old_rank-new_rank)
        totdiff = totdiff + diff

    avediff = totdiff / len(prev_ranks)
    print(i+1, avediff)

    # rotate
    prev_ranks = next_ranks

# Put the final ranks back into the database
print(list(next_ranks.items())[:5])
cur.execute('''UPDATE Pages SET old_rank=new_rank''')
for (id, new_rank) in list(next_ranks.items()) :
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
conn.commit()
cur.close()
