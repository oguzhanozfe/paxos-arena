using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace PaxosArena.Client.Harness
{
    /// <summary>
    /// An independent C# implementation of the deal (section 5) and the rules of
    /// Ladder v1 (section 4), used to check what the server reveals: the
    /// commitment against the seed, the deal against the first view, and the
    /// moves against the final board and score. Never part of the SDK: a client
    /// does not compute outcomes.
    /// </summary>
    public static class LadderAudit
    {
        public const int DeckSize = 52;
        public const int Columns = 7;
        public const int ColumnHeight = 5;
        public const int TableauSize = 35;
        public const string RankChars = "A23456789TJQK";
        public const string SuitChars = "cdhs";

        public static byte[] FromHex(string hex)
        {
            if (hex == null || hex.Length % 2 != 0)
            {
                throw new FormatException("hex of odd length");
            }
            byte[] bytes = new byte[hex.Length / 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                bytes[i] = Convert.ToByte(hex.Substring(2 * i, 2), 16);
            }
            return bytes;
        }

        public static string ToHex(byte[] bytes)
        {
            return Convert.ToHexString(bytes).ToLowerInvariant();
        }

        /// <summary>seed = HMAC-SHA256(secret, "paxos-arena/deal/v1" 0 tid 0 pid 0 round-u32be).</summary>
        public static string DeriveSeed(byte[] secret, string tournament, string player, int round)
        {
            List<byte> input = new List<byte>();
            input.AddRange(Encoding.ASCII.GetBytes("paxos-arena/deal/v1"));
            input.Add(0);
            input.AddRange(Encoding.ASCII.GetBytes(tournament));
            input.Add(0);
            input.AddRange(Encoding.ASCII.GetBytes(player));
            input.Add(0);
            input.Add((byte)(round >> 24));
            input.Add((byte)(round >> 16));
            input.Add((byte)(round >> 8));
            input.Add((byte)round);
            using (HMACSHA256 mac = new HMACSHA256(secret))
            {
                return ToHex(mac.ComputeHash(input.ToArray()));
            }
        }

        /// <summary>commitment = SHA-256("paxos-arena/commit/v1" 0 seed).</summary>
        public static string Commit(string seedHex)
        {
            byte[] prefix = Encoding.ASCII.GetBytes("paxos-arena/commit/v1");
            byte[] seed = FromHex(seedHex);
            byte[] input = new byte[prefix.Length + 1 + seed.Length];
            Buffer.BlockCopy(prefix, 0, input, 0, prefix.Length);
            Buffer.BlockCopy(seed, 0, input, prefix.Length + 1, seed.Length);
            return ToHex(SHA256.HashData(input));
        }

        /// <summary>The deck shuffled from the seed: Fisher-Yates from the top with rejection sampling.</summary>
        public static int[] Shuffle(string seedHex)
        {
            Stream stream = new Stream(FromHex(seedHex));
            int[] deck = new int[DeckSize];
            for (int i = 0; i < DeckSize; i++)
            {
                deck[i] = i;
            }
            for (int i = DeckSize - 1; i >= 1; i--)
            {
                ulong n = (ulong)(i + 1);
                ulong limit = (1UL << 32) - ((1UL << 32) % n);
                ulong x;
                do
                {
                    x = stream.Next32();
                }
                while (x >= limit);
                int j = (int)(x % n);
                int t = deck[i];
                deck[i] = deck[j];
                deck[j] = t;
            }
            return deck;
        }

        public static string CardName(int card)
        {
            return new string(new[] { RankChars[card / 4], SuitChars[card % 4] });
        }

        public static int ParseCard(string name)
        {
            if (name == null || name.Length != 2)
            {
                return -1;
            }
            int rank = RankChars.IndexOf(name[0]);
            int suit = SuitChars.IndexOf(name[1]);
            return rank < 0 || suit < 0 ? -1 : rank * 4 + suit;
        }

        public static int Rank(int card)
        {
            return card / 4 + 1;
        }

        /// <summary>Ranks differ by one, king and ace included.</summary>
        public static bool Adjacent(int a, int b)
        {
            int d = ((Rank(a) - Rank(b)) % 13 + 13) % 13;
            return d == 1 || d == 12;
        }

        /// <summary>The playable columns of a server view, from its cards alone.</summary>
        public static int[] PlayableFromView(RoundView view)
        {
            List<int> result = new List<int>();
            if (view.status != RoundStatus.Playing)
            {
                return result.ToArray();
            }
            int waste = ParseCard(view.waste_top);
            for (int c = 0; c < view.columns.Length; c++)
            {
                string[] cards = view.columns[c].cards;
                if (cards.Length > 0 && Adjacent(ParseCard(cards[cards.Length - 1]), waste))
                {
                    result.Add(c);
                }
            }
            return result.ToArray();
        }

        /// <summary>The block stream SHA-256(seed || k as u64be).</summary>
        sealed class Stream
        {
            readonly byte[] seed;
            byte[] block = new byte[0];
            int offset;
            ulong k;

            public Stream(byte[] seed)
            {
                this.seed = seed;
            }

            byte NextByte()
            {
                if (offset == block.Length)
                {
                    byte[] input = new byte[seed.Length + 8];
                    Buffer.BlockCopy(seed, 0, input, 0, seed.Length);
                    for (int i = 0; i < 8; i++)
                    {
                        input[seed.Length + i] = (byte)(k >> (56 - 8 * i));
                    }
                    block = SHA256.HashData(input);
                    offset = 0;
                    k++;
                }
                return block[offset++];
            }

            public ulong Next32()
            {
                ulong x = 0;
                for (int i = 0; i < 4; i++)
                {
                    x = (x << 8) | NextByte();
                }
                return x;
            }
        }

        /// <summary>A Ladder v1 board replayed locally.</summary>
        public sealed class Board
        {
            public readonly List<int>[] Tableau = new List<int>[Columns];
            public readonly List<int> Waste = new List<int>();
            readonly int[] deck;
            int stockNext = TableauSize + 1;

            public Board(int[] deck)
            {
                this.deck = deck;
                for (int c = 0; c < Columns; c++)
                {
                    Tableau[c] = new List<int>();
                    for (int k = 0; k < ColumnHeight; k++)
                    {
                        Tableau[c].Add(deck[ColumnHeight * c + k]);
                    }
                }
                Waste.Add(deck[TableauSize]);
            }

            public int StockCount
            {
                get { return DeckSize - stockNext; }
            }

            public int WasteTop
            {
                get { return Waste[Waste.Count - 1]; }
            }

            public int Cleared
            {
                get
                {
                    int left = 0;
                    for (int c = 0; c < Columns; c++)
                    {
                        left += Tableau[c].Count;
                    }
                    return TableauSize - left;
                }
            }

            public long Score
            {
                get
                {
                    int cleared = Cleared;
                    return 100L * cleared + (cleared == TableauSize ? 500 + 50L * StockCount : 0);
                }
            }

            public int[] Playable()
            {
                List<int> result = new List<int>();
                for (int c = 0; c < Columns; c++)
                {
                    List<int> column = Tableau[c];
                    if (column.Count > 0 && Adjacent(column[column.Count - 1], WasteTop))
                    {
                        result.Add(c);
                    }
                }
                return result.ToArray();
            }

            /// <summary>"cleared", "blocked", or "" while the round goes on.</summary>
            public string Over()
            {
                if (Cleared == TableauSize)
                {
                    return FinishReason.Cleared;
                }
                if (StockCount == 0 && Playable().Length == 0)
                {
                    return FinishReason.Blocked;
                }
                return "";
            }

            /// <summary>Applies a move; false when it is illegal or the round is over.</summary>
            public bool Apply(string kind, int column)
            {
                if (Over().Length > 0)
                {
                    return false;
                }
                if (kind == MoveKind.Draw)
                {
                    if (StockCount == 0)
                    {
                        return false;
                    }
                    Waste.Add(deck[stockNext++]);
                    return true;
                }
                if (kind != MoveKind.Play || column < 0 || column >= Columns)
                {
                    return false;
                }
                List<int> cards = Tableau[column];
                if (cards.Count == 0 || !Adjacent(cards[cards.Count - 1], WasteTop))
                {
                    return false;
                }
                Waste.Add(cards[cards.Count - 1]);
                cards.RemoveAt(cards.Count - 1);
                return true;
            }

            /// <summary>Returns "" when the server's view shows this board, or the first difference.</summary>
            public string Compare(RoundView view)
            {
                if (view.columns.Length != Columns)
                {
                    return "view has " + view.columns.Length + " columns";
                }
                for (int c = 0; c < Columns; c++)
                {
                    string[] cards = view.columns[c].cards;
                    if (cards.Length != Tableau[c].Count)
                    {
                        return "column " + c + " has " + cards.Length + " cards, expected " + Tableau[c].Count;
                    }
                    for (int k = 0; k < cards.Length; k++)
                    {
                        if (cards[k] != CardName(Tableau[c][k]))
                        {
                            return "column " + c + " card " + k + " is " + cards[k] + ", expected " + CardName(Tableau[c][k]);
                        }
                    }
                }
                if (view.waste_top != CardName(WasteTop))
                {
                    return "waste card " + view.waste_top + ", expected " + CardName(WasteTop);
                }
                if (view.waste_count != Waste.Count)
                {
                    return "waste count " + view.waste_count + ", expected " + Waste.Count;
                }
                if (view.stock_count != StockCount)
                {
                    return "stock count " + view.stock_count + ", expected " + StockCount;
                }
                if (view.cleared != Cleared)
                {
                    return "cleared " + view.cleared + ", expected " + Cleared;
                }
                if (view.score != Score)
                {
                    return "score " + view.score + ", expected " + Score;
                }
                return "";
            }
        }
    }
}
