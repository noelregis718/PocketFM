import pandas as pd
import re
import os
import format_excel

EXCEL_FILE = r"e:\Internship\PocketFM\Combined_Scraped_Data.xlsx"

DEALBREAKERS = [
    "science fiction", "sci-fi", "childrens", "middle grade", 
    "historical non-fiction", "biography", "office romance", "billionaire romance"
]

GOLD_AUTHORS = [
    "sarah j. maas", "rebecca yarros", "jennifer l. armentrout", "laura thalassa", 
    "raven kennedy", "carissa broadbent", "scarlett st. clair", "lexi ryan", 
    "elise kova", "ali hazelwood", "kresley cole", "s.j. maas", "jennifer armentrout"
]

SUBGENRES = {
    "High Fantasy Court Adventure": {
        "confirmed": ["fae court romance", "royal fantasy romance", "fantasy court romance", "kingdom romance", "royal heir romance", "noble fantasy romance", "unseelie court", "seelie court"],
        "master": ["royal court fantasy", "court intrigue", "political fantasy", "kingdom politics", "noble houses", "royal succession", "crown politics", "throne war", "castle court", "imperial court", "court mage", "royal heir", "kingdom alliance", "noble romance", "palace fantasy", "throne of", "crown of", "king and queen", "royal romance", "fae king", "prince romance", "princess romance", "royal intrigue", "court secrets", "fae prince"]
    },
    "Gothic Dark Romantasy": {
        "confirmed": ["gothic romance", "dark fantasy romance", "cursed castle romance", "gothic fantasy romance", "gothic thriller"],
        "master": ["gothic romance", "haunted castle", "haunted manor", "dark curse", "cursed castle", "victorian fantasy", "grim romance", "gothic horror fantasy", "dark cathedral", "shadow manor", "tragic love fantasy", "macabre romance", "gothic manor", "dark estate", "gothic supernatural", "death romance", "forbidden desire", "undead romance", "haunted house romance", "dark lord", "forbidden love", "dark obsession", "monstrous lover", "dark magic", "shadow magic", "blood magic", "grimdark romance", "villain romance"]
    },
    "Dark Academia Romantasy": {
        "confirmed": ["dark academia romance", "occult academy romance", "forbidden magic academy", "secret society romance", "cursed university romance"],
        "master": ["dark academia", "secret society", "forbidden knowledge", "occult studies", "elite academy", "cursed library", "forbidden ritual", "ancient grimoire", "magic university", "arcane college", "sinister campus", "forbidden spellwork", "hidden magic society", "dangerous knowledge", "magical conspiracy", "ink magic", "tarot academy", "clandestine order", "ivy league magic", "magical underground", "magical school", "dark school", "ancient magic", "secret magic", "deadly academy", "magic scholars", "scholar romance"]
    },
    "Monster Romance (Non-Shifter)": {
        "confirmed": ["monster romance", "demon romance", "monster boyfriend", "alien monster romance", "non-human romance", "orc romance", "gargoyle romance", "naga romance"],
        "master": ["monster romance", "demon romance", "monster boyfriend", "vampire romance", "fae romance", "mer romance", "kraken romance", "minotaur romance", "alien romance", "creature romance", "non-human love interest", "monstrous partner", "immortal captor", "beast romance", "alien mate", "spider romance", "monster lover", "creature feature romance", "human and monster", "symbiote romance"]
    },
    "Werewolf / Shifter Romance": {
        "confirmed": ["werewolf romance", "shifter romance", "fated mates", "wolf shifter romance", "rejected mate romance", "shifter mate"],
        "master": ["werewolf romance", "shifter romance", "fated mates", "rejected mate", "alpha romance", "lycan romance", "wolf pack romance", "bear shifter romance", "dragon shifter romance", "omegaverse romance", "pack romance", "mate bond", "true mate", "shifter pack", "animal shifter", "werebear", "werelion", "werepanther", "werecat", "alpha male", "luna", "pack dynamics", "wolf pack"]
    },
    "High-Stakes Games & Deadly Trials": {
        "confirmed": ["fantasy tournament romance", "progression fantasy romance", "dungeon trials romance", "deadly trials romance", "death game romance", "magic tournament", "deadly games"],
        "master": ["deadly trials", "magical tournament", "battle tournament", "survival trials", "death competition", "trial by combat", "fantasy arena", "magic competition", "elimination tournament", "dungeon trials", "labyrinth challenge", "champion tournament", "fantasy contest", "survival arena", "final trial", "death game", "magical contest", "forced competition", "win or die", "chosen champion", "trial of", "survival romance", "blood tournament", "combat trials", "gladiator romance"]
    },
    "Mythology, Legend & Fairy Tale Retelling": {
        "confirmed": ["beauty and the beast retelling", "hades and persephone", "greek mythology romance", "fairy tale retelling romance", "myth retelling romance"],
        "master": ["greek myth retelling", "norse myth retelling", "celtic legend", "arthurian legend", "fairy tale retelling", "hades and persephone", "cinderella retelling", "sleeping beauty retelling", "red riding hood retelling", "snow white retelling", "rapunzel retelling", "medusa retelling", "persephone retelling", "mythic retelling", "chinese mythology romance", "folklore retelling", "legend reimagined", "fairy tale romance", "myth romance", "goddess romance", "greek myth", "roman myth", "norse myth", "egyptian myth", "celtic myth", "arthurian romance", "camelot romance", "robin hood retelling", "peter pan retelling", "alice in wonderland retelling", "beauty and the beast", "rumplestiltskin retelling"]
    },
    "War College / Military Academy": {
        "confirmed": ["magic academy romance", "military fantasy romance", "war college romance", "dragon rider academy romance", "combat academy romance"],
        "master": ["war college", "military academy", "officer academy", "cadet academy", "battle school", "combat academy", "warrior academy", "military fantasy", "officer training", "academy cadets", "battle training", "tactical academy", "war strategy", "military campaign", "combat training", "rider academy", "dragon rider", "flight academy", "wingleader", "cadet romance", "dragon school", "battle academy", "warrior school", "magic soldier", "warrior romance"]
    },
    "Korean Romance Fantasy / Isekai": {
        "confirmed": ["villainess romance", "isekai romance", "litrpg romance", "transmigration romance", "reincarnation fantasy romance"],
        "master": ["otome isekai", "villainess", "reincarnated heroine", "transmigration", "regression fantasy", "reborn noblewoman", "korean web novel", "manhwa romance", "duke of the north", "tyrant romance", "second chance fantasy", "possession fantasy", "system fantasy romance", "korean fantasy novel", "isekai romance", "portal fantasy romance", "game world romance", "reincarnated into novel", "manhwa adaptation", "web novel romance", "transmigrated", "otome game", "webtoon romance", "manhwa", "regression", "time travel romance", "reborn as"]
    },
    "Paranormal Romance": {
        "confirmed": ["vampire romance", "witch romance", "angel romance", "ghost romance", "psychic romance", "warlock romance"],
        "master": ["vampire romance", "witch romance", "demon romance", "angel romance", "ghost romance", "psychic romance", "necromancer romance", "fallen angel romance", "supernatural romance", "immortal romance", "soul bond", "witch coven romance", "vampire mate", "dark angel", "haunting romance", "vampire hunter", "witch coven", "demon hunter", "angel and demon", "nephilim", "shifter and witch"]
    },
    "Cozy / Cottagecore Romantasy": {
        "confirmed": ["cozy fantasy romance", "cottagecore romance", "cozy witch romance", "magical small town romance", "cozy romantasy", "cozy magic", "slice of life fantasy", "witchy cozy"],
        "master": ["cozy fantasy", "cottagecore", "small town magic", "cozy witch", "magical bakery", "enchanted garden", "herbalist romance", "cottage magic", "whimsical romance", "gentle magic", "warm magic", "magical community", "village witch", "low-stakes magic", "healing magic", "nature magic romance", "magical shop", "bookshop magic", "woodland cottage", "cozy magical world", "low stakes fantasy", "magical baking", "magical tea shop", "cozy mystery with magic"]
    },
    "Urban / Contemporary Fantasy Romance": {
        "confirmed": ["urban fantasy romance", "hidden magic romance", "urban coven romance", "secret magic world", "contemporary fantasy romance"],
        "master": ["urban fantasy", "contemporary fantasy", "modern magic", "hidden magic society", "magical city", "supernatural detective", "paranormal investigation", "secret magic world", "modern sorcerer", "magical crime", "city witch", "urban coven", "magical apartment", "urban spellcaster", "metropolitan fantasy", "hidden world", "magic underground", "supernatural organisation", "modern witch", "city fae", "magic in the city", "masquerade", "supernatural city", "modern day magic", "detective witch", "paranormal PI"]
    }
}

def clean_text(val):
    if pd.isna(val):
        return ""
    val = str(val).lower()
    return re.sub(r'[^a-z0-9\s]', ' ', val)

def count_matches(word_list, text):
    count = 0
    for word in word_list:
        # NLP Regex Suffix Expansion: matches exact word, optionally followed by common linguistic suffixes 
        # (s, es, ed, d, ing, ly, al, y)
        pattern = r'\b' + re.escape(word.lower()) + r'(?:s|es|ed|d|ing|ly|al|y)?\b'
        if re.search(pattern, text):
            count += 1
    return count

def run_checker():
    print(f"Loading {EXCEL_FILE}...")
    df = pd.read_excel(EXCEL_FILE)
    
    if "Romantasy Checker" not in df.columns:
        df["Romantasy Checker"] = None

    print(f"Checking {len(df)} rows for subgenres (V3)...")
    for idx, row in df.iterrows():
        # Get individual column text
        col_title = clean_text(row.get('Book Title'))
        col_tags = clean_text(row.get('Genre Tags'))
        col_series = clean_text(row.get('Series Name'))
        col_logline = clean_text(row.get('Logline'))
        col_synopsis = clean_text(row.get('Synopsis'))
        col_author = clean_text(row.get('Author Name'))
        
        # Check if the author is a Gold Standard Romantasy author
        is_gold_author = any(a in col_author for a in GOLD_AUTHORS)
        
        # 1. Negative Dealbreaker Check
        combined_text = f"{col_title} {col_tags} {col_series} {col_logline} {col_synopsis}"
        is_dealbreaker = False
        for db in DEALBREAKERS:
            if re.search(r'\b' + re.escape(db) + r'\b', combined_text):
                is_dealbreaker = True
                break
                
        # If it's a Gold Author, we NEVER fail them on a dealbreaker (e.g. SJM wrote a sci-fi romantasy)
        if is_dealbreaker and not is_gold_author:
            df.at[idx, 'Romantasy Checker'] = 'Fail (Dealbreaker)'
            continue
            
        scores = []
        for genre_name, keywords in SUBGENRES.items():
            conf_count = 0
            mast_count = 0
            total_points = 0
            
            # --- CONFIRMED KEYWORDS (Base +3 Points) ---
            for word in keywords['confirmed']:
                # x3 Multiplier Columns (Title, Tags)
                if count_matches([word], col_title) or count_matches([word], col_tags):
                    conf_count += 1
                    total_points += (3 * 3)
                # x2 Multiplier Columns (Series, Logline)
                elif count_matches([word], col_series) or count_matches([word], col_logline):
                    conf_count += 1
                    total_points += (3 * 2)
                # x1 Multiplier Column (Synopsis)
                elif count_matches([word], col_synopsis):
                    conf_count += 1
                    total_points += (3 * 1)
                    
            # --- MASTER KEYWORDS (Base +1 Point) ---
            for word in keywords['master']:
                if count_matches([word], col_title) or count_matches([word], col_tags):
                    mast_count += 1
                    total_points += (1 * 3)
                elif count_matches([word], col_series) or count_matches([word], col_logline):
                    mast_count += 1
                    total_points += (1 * 2)
                elif count_matches([word], col_synopsis):
                    mast_count += 1
                    total_points += (1 * 1)
                    
            if total_points > 0:
                scores.append({
                    'genre': genre_name,
                    'points': total_points,
                    'confirmed': conf_count,
                    'master': mast_count
                })
                
        if not scores:
            if is_gold_author:
                # If they scored 0 but are a Gold Author, default to Strong Match (Uncategorized)
                df.at[idx, 'Romantasy Checker'] = 'Strong Match (High Fantasy Court Adventure)'
            else:
                df.at[idx, 'Romantasy Checker'] = 'Fail'
        else:
            scores.sort(key=lambda x: (x['points'], x['confirmed'], x['master']), reverse=True)
            winner = scores[0]
            
            # If the winning score is 3 or more, OR if they are a Gold Author, it's a Strong Match
            if winner['points'] >= 3 or is_gold_author:
                df.at[idx, 'Romantasy Checker'] = f"Strong Match ({winner['genre']})"
            else:
                df.at[idx, 'Romantasy Checker'] = f"Weak Match ({winner['genre']})"
                
        if (idx + 1) % 1000 == 0:
            print(f"Processed {idx + 1} rows...")

    print("Saving updated file...")
    df.to_excel(EXCEL_FILE, index=False)
    
    print("Applying styling...")
    format_excel.apply_styling(EXCEL_FILE)
    print("Complete! Subgenres assigned successfully with V3 Logic.")

if __name__ == "__main__":
    run_checker()
