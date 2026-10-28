"""MudaeParser engine tests — dataset-grounded parsing (spec: every
message family from CuratedMudaeDataset parses with BUILT-IN DEFAULTS,
no settings file required; unknown messages never raise)."""
from __future__ import annotations

import json
import os
import unittest
from types import SimpleNamespace

from autoclaimer.mudae_parser import (
    MessageFamily,
    MudaeParser,
    MudaeParserSettingsManager,
)
from autoclaimer.mudae_parser.settings.storage import MemorySettingsStorage

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATASET = os.path.join(REPO, "parsers", "CuratedMudaeDataset")


def make_embed(author_name=None, title=None, description=None,
               footer=None, fields=()):
    return SimpleNamespace(
        author=(SimpleNamespace(name=author_name)
                if author_name is not None else None),
        title=title,
        description=description,
        footer=(SimpleNamespace(text=footer)
                if footer is not None else None),
        fields=[SimpleNamespace(name=n, value=v, inline=i)
                for n, v, i in fields],
    )


def make_message(content=None, embeds=()):
    return SimpleNamespace(content=content, embeds=list(embeds),
                           components=())


def fresh_parser() -> MudaeParser:
    manager = MudaeParserSettingsManager(storage=MemorySettingsStorage())
    return MudaeParser(manager)


class CharacterRollTests(unittest.TestCase):
    """rolls.jsonl evidence: author=name, desc=series+stats, footer=owner."""

    def setUp(self):
        self.parser = fresh_parser()

    def test_owned_roll_embed(self):
        message = make_message(embeds=[make_embed(
            author_name="Ena Shinonome",
            description="Project SEKAI: Colorful Stage! | Claims: #892 | "
                        "Likes: #689 | **1,043**"
                        "<:kakera:469835869059153940>",
            footer="Belongs to libosterelezent")])
        result = self.parser.parse(message)
        self.assertIs(result.family, MessageFamily.CHARACTER)
        char = result.character
        self.assertEqual(char.name, "Ena Shinonome")
        self.assertEqual(char.series, "Project SEKAI: Colorful Stage!")
        self.assertEqual(char.claims_rank, 892)
        self.assertEqual(char.likes_rank, 689)
        self.assertEqual(char.kakera_value, 1043)
        self.assertEqual(char.claimed_by, "libosterelezent")
        self.assertIs(char.claimable, False)

    def test_keyed_starwished_roll(self):
        description = (
            "GODDESS OF VICTORY: NIKKE <:sw:1163913219782492220> | "
            "<:chaoskey:690110264166842421> (**5,368**) +5% kakera value | "
            "<:chaoskey:690110264166842421> (**5,369**) +5% kakera value | "
            "Claims: #93,023 | Likes: #67,111 | "
            "**2,499,848**<:kakera:469835869059153940>")
        result = self.parser.parse(make_message(
            embeds=[make_embed(author_name="Character",
                               description=description)]))
        char = result.character
        self.assertEqual(char.series, "GODDESS OF VICTORY: NIKKE")
        self.assertEqual(char.starwish_count, 1)
        self.assertEqual(len(char.keys), 2)
        self.assertEqual(char.keys[0].key_number, 5368)
        self.assertEqual(char.keys[0].key_type, "chaos")
        self.assertEqual(char.keys[0].value_pct, 5)
        self.assertEqual(char.kakera_value, 2499848)
        self.assertIsNone(char.claimed_by)

    def test_silver_key_info_layout(self):
        description = (
            "Genshin Impact  <:female:452463537508450304>  | *Game "
            "roulette*  · **542,501**<:kakera:469835869059153940>**  · "
            "<:silverkey:690110264166842421>  (1,476)  | Claim Rank: #521 "
            "| Like Rank: #613")
        result = self.parser.parse(make_message(
            embeds=[make_embed(author_name="Mika",
                               description=description)]))
        char = result.character
        self.assertEqual(char.claims_rank, 521)      # alt layout honored
        self.assertEqual(char.likes_rank, 613)
        self.assertEqual(char.kakera_value, 542501)
        self.assertEqual(char.keys[0].key_type, "silver")
        self.assertIsNone(char.keys[0].value_pct)

    def test_claimable_roll(self):
        """Live-verified react-to-claim server variant."""
        result = self.parser.parse(make_message(embeds=[make_embed(
            author_name="Makio Fujiyama",
            description="Fujiyama-san wa Shishunki | Claims: #73,398 | "
                        "Likes: #52,933 | **493**"
                        "<:kakera:469835869059153940> | React with any "
                        "emoji to claim!")]))
        self.assertIs(result.character.claimable, True)
        self.assertIsNone(result.character.claimed_by)


class KakeraAndLootTests(unittest.TestCase):
    def setUp(self):
        self.parser = fresh_parser()

    def test_kakera_reaction(self):
        result = self.parser.parse(
            "<:kakeraP:609264156347990016>(Free) **htbao7 +700** ($k)")
        self.assertIs(result.family, MessageFamily.KAKERA)
        event = result.kakera_events[0]
        self.assertEqual(event.amount, 700)
        self.assertEqual(event.username, "htbao7")

    def test_kakera_reaction_colored_variant(self):
        """kakeraY (not just kakeraP) — dataset color variants."""
        result = self.parser.parse(
            "<:kakeraY:633217516461883404>(Free) **smashxy +1,926** ($k)")
        self.assertIs(result.family, MessageFamily.KAKERA)
        self.assertEqual(result.kakera_events[0].amount, 1926)

    def test_kakera_reaction_kakera_d_prefix(self):
        result = self.parser.parse(
            "<:kakeraD:1436810949548839003>turns into"
            "<:kakeraP:609264156347990016>\n"
            "<:kakeraP:609264156347990016>(Free) **.nickend +350** ($k) "
            "**+24** <:sp:1437140700604137554>")
        event = result.kakera_events[0]
        self.assertEqual(event.username, ".nickend")
        self.assertEqual(event.spheres, 24)

    def test_kakera_reaction_paid_variant(self):
        """Live-verified: paid reactions omit the (Free) marker."""
        result = self.parser.parse(
            "<:kakeraO:1097914854318433451> **libosterelezent +5,740** "
            "($k) **+15** <:sp:1437140700604137554>")
        self.assertIs(result.family, MessageFamily.KAKERA)
        event = result.kakera_events[0]
        self.assertEqual(event.amount, 5740)
        self.assertEqual(event.username, "libosterelezent")
        self.assertEqual(event.spheres, 15)

    def test_sphere_click(self):
        """Live-verified: ouro-sphere button gains."""
        result = self.parser.parse(
            "<:spT:1437140651614535680> **libosterelezent +51**  (1/15)")
        self.assertIs(result.family, MessageFamily.KAKERA)
        event = result.kakera_events[0]
        self.assertEqual(event.kind.value, "sphere_click")
        self.assertEqual(event.amount, 51)

    def test_dk_ready(self):
        """Live-verified: $dk ready marker."""
        result = self.parser.parse("$dk is ready!")
        self.assertIs(result.family, MessageFamily.COOLDOWN)
        cooldown = result.cooldowns[0]
        self.assertEqual(cooldown.kind.value, "daily")
        self.assertIsNone(cooldown.remaining)

    def test_custom_claim_message(self):
        """Live-verified custom claim message ($setclaimmessage)."""
        result = self.parser.parse(
            "「**libosterelezent**」added「**Zero Two**」into the "
            "collection. ✨ | **+2,205,453,173**"
            "<:kakera:469835869059153940>")
        self.assertIs(result.family, MessageFamily.CLAIM)
        event = result.kakera_events[0]
        self.assertEqual(event.kind.value, "claim_gain")
        self.assertEqual(event.amount, 2205453173)
        self.assertEqual(event.metadata["character"], "Zero Two")

    def test_kakeraloot_payout(self):
        result = self.parser.parse(
            "<:rollstack:633217516461883404> **+9,601.2** rolls stacked "
            "(Stock: 957,055,879.6)\n"
            "<:morekakera:633217512057864192> **+1,282,329** kakera")
        self.assertIs(result.family, MessageFamily.KAKERALOOT)
        self.assertEqual(result.kakeraloot.kakera_gained, 1282329)
        self.assertEqual(result.kakeraloot.rolls_stacked, 9601.2)
        self.assertEqual(result.kakeraloot.stock, 957055879.6)

    def test_kakeraloot_profile_title(self):
        result = self.parser.parse(make_message(embeds=[make_embed(
            title="joetion - Kakeraloots",
            description="<:rollstack:633217516461883404> Rolls stacked: "
                        "**122,509,223** ($us)")]))
        self.assertIs(result.family, MessageFamily.KAKERALOOT)
        self.assertEqual(result.kakeraloot.username, "joetion")
        self.assertIn("joetion", result.usernames)


class CooldownTests(unittest.TestCase):
    def setUp(self):
        self.parser = fresh_parser()

    def test_roulette_limit(self):
        result = self.parser.parse(
            "**emporos21**, the roulette is limited to 107 uses per "
            "hour. **16** min left. | Upvote Mudae to reset the timer: "
            "**$vote**. Website: <https://mudae.net>")
        self.assertIs(result.family, MessageFamily.COOLDOWN)
        cooldown = result.cooldowns[0]
        self.assertEqual(cooldown.metadata["uses_per_hour"], 107)
        self.assertEqual(cooldown.remaining.total_seconds(), 960)
        self.assertEqual(cooldown.username, "emporos21")

    def test_rt_cooldown_compound(self):
        result = self.parser.parse(
            "The cooldown of $rt is not over. Time left: **2h 59** min. "
            "($rtu)")
        cooldown = result.cooldowns[0]
        self.assertEqual(cooldown.remaining.total_seconds(), 10740)

    def test_rt_cooldown_plain(self):
        result = self.parser.parse(
            "The cooldown of $rt is not over. Time left: **41** min. "
            "($rtu)")
        self.assertEqual(result.cooldowns[0].remaining.total_seconds(),
                         2460)

    def test_rt_available_missing_duration_is_not_zero(self):
        result = self.parser.parse("$rt is available!")
        cooldown = result.cooldowns[0]
        self.assertIsNone(cooldown.remaining)  # missing != zero
        self.assertTrue(cooldown.metadata["available"])

    def test_react_cooldown(self):
        result = self.parser.parse(
            "**.nickend**, You can't react to kakera for **14** min. "
            "($ku)")
        cooldown = result.cooldowns[0]
        self.assertEqual(cooldown.username, ".nickend")
        self.assertEqual(cooldown.remaining.total_seconds(), 840)

    def test_react_cooldown_compound(self):
        result = self.parser.parse(
            "**user**, You can't react to kakera for **2h 14** min. "
            "($ku)")
        self.assertEqual(result.cooldowns[0].remaining.total_seconds(),
                         8040)

    def test_claim_timer_full(self):
        result = self.parser.parse(
            "**nickendz**, you __can__ claim right now! The next claim "
            "reset is in **8** min.\nYou have **61** rolls (+**3** $mk) "
            "left. Next rolls reset in **8** min.\n\n\nYou __can__ react "
            "to kakera right now! Power: **99%** Each kakera button "
            "consumes 1% of your reaction power.")
        self.assertIs(result.family, MessageFamily.TIMER)
        self.assertTrue(result.timer.can_claim)
        self.assertEqual(result.timer.claim_resets_in.total_seconds(), 480)
        self.assertEqual(result.timer.rolls_left, 61)
        self.assertEqual(result.timer.mk_rolls, 3)
        self.assertTrue(result.timer.can_react)
        self.assertEqual(result.timer.react_power_pct, 99)
        self.assertEqual(len(result.cooldowns), 2)

    def test_claim_timer_cant_claim_compound(self):
        result = self.parser.parse(
            "**rubenpfdez05**, you can't claim for another **1h 43** "
            "min.\nYou have **3** rolls (+**0** $mk) left. Next rolls "
            "reset in **43** min.\n\nYou can't react to kakera for "
            "**2h 30** min. Power: **41%**")
        self.assertFalse(result.timer.can_claim)
        self.assertEqual(result.timer.claim_resets_in.total_seconds(),
                         6180)
        self.assertFalse(result.timer.can_react)
        self.assertEqual(len(result.cooldowns), 3)


class TradeWishlistMarriageTests(unittest.TestCase):
    def setUp(self):
        self.parser = fresh_parser()

    def test_trade_deficit(self):
        result = self.parser.parse(
            "You need an additional **1,252,294**"
            "<:kakera:469835869059153940>!\nType **$infokl** to get more "
            "infos about this command.")
        self.assertIs(result.family, MessageFamily.TRADE)
        self.assertEqual(result.trade.amount, 1252294)

    def test_gift_kakera(self):
        result = self.parser.parse(
            "<@873911093934829664> just gifted **20,000,000,000**"
            "<:kakera:469835869059153940>to <@1192119301503721533>")
        trade = result.trade
        self.assertEqual(trade.amount, 20000000000)
        self.assertEqual(trade.from_user_id, 873911093934829664)
        self.assertEqual(trade.to_user_id, 1192119301503721533)

    def test_gift_spheres_variant(self):
        result = self.parser.parse(
            "<@111> just gifted **500** <:sp:1437140700604137554> to "
            "<@222>")
        self.assertEqual(result.trade.amount, 500)

    def test_give_confirm_kakera(self):
        result = self.parser.parse(
            "**renma.1**, do you really want to give **20,000,000,000"
            "<:kakera:469835869059153940>**? (y/n/yes/no)")
        self.assertEqual(result.trade.username, "renma.1")
        self.assertEqual(result.trade.amount, 20000000000)

    def test_give_confirm_spheres(self):
        result = self.parser.parse(
            "**itz.fahim**, do you really want to give **1,000 "
            "<:sp:1437140700604137554> **? (y/n/yes/no)")
        self.assertEqual(result.trade.amount, 1000)

    def test_wishlist_refill(self):
        result = self.parser.parse(
            "**0** $oh left for today, **0** $oc, **0** $oq and **0** "
            "$ot.\n**9h 38** min before the refill.\nStock: **38,454** "
            "<:sp:1437140700604137554>")
        self.assertIs(result.family, MessageFamily.WISHLIST)
        self.assertEqual(result.wishlist.stock, 38454)
        self.assertEqual(result.wishlist.refill_in.total_seconds(), 34680)

    def test_marriage(self):
        result = self.parser.parse(
            "💐 **joon07584** and **Crimson-Haired Ruin Princess** are "
            "now married! 💐\n**+364,524,120**"
            "<:kakera:469835869059153940>(Emerald IV bonus) +**262** "
            "<:sp:1437140700604137554>")
        marriage = result.marriage
        self.assertEqual(marriage.user_a, "joon07584")
        self.assertEqual(marriage.character, "Crimson-Haired Ruin "
                                             "Princess")
        self.assertEqual(marriage.kakera_bonus, 364524120)
        self.assertEqual(marriage.tier, "Emerald IV")


class UsernameContextTests(unittest.TestCase):
    def setUp(self):
        self.parser = fresh_parser()

    def test_extracted_username_never_replaced_by_context(self):
        result = self.parser.parse(
            "**emporos21**, the roulette is limited to 5 uses per hour. "
            "**4** min left.",
            account_username="my_main_account")
        self.assertIn("emporos21", result.usernames)
        self.assertNotIn("my_main_account", result.usernames)
        self.assertEqual(result.account_username, "my_main_account")

    def test_no_fabricated_identity(self):
        result = self.parser.parse("$rt is available!",
                                   account_username="ctx")
        self.assertEqual(result.usernames, ())
        self.assertEqual(result.account_username, "ctx")


class NegativeAndReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.parser = fresh_parser()

    def test_unknown_message_is_not_an_error(self):
        result = self.parser.parse("some random conversation message")
        self.assertIs(result.family, MessageFamily.UNKNOWN)
        self.assertTrue(result.diagnostics.has("unknown_format"))

    def test_empty_string_unknown(self):
        result = self.parser.parse("")
        self.assertIs(result.family, MessageFamily.UNKNOWN)

    def test_none_raises_invalid(self):
        from autoclaimer.mudae_parser.exceptions import InvalidMessageError

        with self.assertRaises(InvalidMessageError):
            self.parser.parse(None)

    def test_unrelated_object_raises_invalid(self):
        from autoclaimer.mudae_parser.exceptions import InvalidMessageError

        with self.assertRaises(InvalidMessageError):
            self.parser.parse(12345)

    def test_oversized_input_is_bounded_not_fatal(self):
        result = self.parser.parse("x" * 500_000)
        self.assertIs(result.family, MessageFamily.UNKNOWN)

    def test_consecutive_calls_consistent(self):
        message = ("<:kakeraP:609264156347990016>(Free) **htbao7 +700**"
                   " ($k)")
        first = self.parser.parse(message)
        second = self.parser.parse(message)
        self.assertEqual(first.to_dict(), second.to_dict())

    def test_independent_instances_consistent(self):
        message = make_message(embeds=[make_embed(
            author_name="A",
            description="S | Claims: #1 | Likes: #2 | **3**"
                        "<:kakera:469835869059153940>")])
        a = fresh_parser().parse(message)
        b = fresh_parser().parse(message)
        self.assertEqual(a.character.to_dict(), b.character.to_dict())

    def test_source_collision_prevention(self):
        """Same text in embed title vs component label never collides:
        the kakeraloot profile pattern targets EMBED_TITLE only."""
        message = SimpleNamespace(
            content=None,
            embeds=[make_embed(title="unrelated")],
            components=[SimpleNamespace(children=[
                SimpleNamespace(label="joetion - Kakeraloots",
                                custom_id=None)])])
        result = self.parser.parse(message)
        self.assertIsNot(result.family, MessageFamily.KAKERALOOT)


class LiveCaptureRegressionTests(unittest.TestCase):
    """The 2026-10-20 live capture (Bazaak's Mudae Lab) — real Discord
    data, skipped gracefully when the file is absent."""

    CAPTURE = os.path.join(DATASET, "live_capture_bazaak_lab.jsonl")

    def test_live_capture(self):
        try:
            with open(self.CAPTURE, encoding="utf-8") as handle:
                records = [json.loads(line) for line in handle]
        except FileNotFoundError:
            self.skipTest("live capture not present")
        parser = fresh_parser()
        expect = {
            4: MessageFamily.TIMER,
            15: MessageFamily.CHARACTER,
            18: MessageFamily.CHARACTER,
            19: MessageFamily.CHARACTER,
            21: MessageFamily.CHARACTER,
            22: MessageFamily.CHARACTER,
        }
        for index, record in enumerate(records):
            message = make_message(
                content=record.get("content") or "",
                embeds=[make_embed(
                    author_name=e.get("author_name"),
                    title=e.get("title"),
                    description=e.get("description"),
                    footer=e.get("footer"))
                    for e in record.get("embeds", [])])
            result = parser.parse(message)
            wanted = expect.get(index)
            if wanted is not None:
                self.assertIs(result.family, wanted,
                              f"record {index}: {result.family}")
            self.assertIsNotNone(result.family)


if __name__ == "__main__":
    unittest.main()
