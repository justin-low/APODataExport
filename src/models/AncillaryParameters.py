from enum import Enum


# Bundles Variants
class Bundles:
    STARTER = [
        'STRT'
    ]
    STARTER_PLUS = [
        'STPL'
    ]
    FLEX = [
        'FLEX', 'FLXN'
    ]
    FLEX_PLUS = [
        'FPLS'
    ]
    MAX = [
        'MAX', 'MAX2'
    ]
    BUSINESS = [
        'BIZZ', 'BMAX', 'BMXN', 'BUS', 'BMA'
    ]
    BUSINESS_PLUS = [
        'PLUS', 'PLS'
    ]


# Seat Variants
class Seats:
    STF = ["STF"]


# Bag Variants
class Bags:
    # Check in Baggage
    PRE_PAID_CHECK_IN = [
        'BF10', 'BF15', 'BF20', 'BF23', 'BF25', 'BF30', 'BF40',
        'BG05', 'BG10', 'BG15', 'BG20', 'BG23', 'BG25', 'BG30', 'BG32', 'BG35', 'BG40', 'BG45', 'BG46', 'BG60'
    ]
    POST_PAID_CHECK_IN = [
        'XB05', 'XB10', 'XB15', 'XB20', 'XB25', 'XB30', 'XB35', 'XB40', 'XB50', 'XB60', 'XB70', 'XB80'
    ]
    # Cabin Baggage
    PRE_PAID_CABIN = [
        'CB03', 'CB07', 'CB10', 'CB14', 'CB20'
    ]
    POST_PAID_CABIN = [
        'CBX3', 'CBX7'
    ]
    COUNTER_CABIN = [
        'CBA3', 'CBA7'
    ]
    GATE_CABIN = [
        'CBG3', 'CBG7'
    ]
    # Excess Baggage
    EXCESS = [
        'EXB',
        'X05', 'X10', 'X20', 'XBGT', 'XBPU', 'XBF', 'XBU'
    ]
    # Oversized
    PRE_PAID_OVERSIZED = [
        'OB01', 'OB02'
    ]
    COUNTER_OVERSIZED = [
        'OBAP'
    ]

# Class Meal Variants
class Meals:
    # Children
    CHILDREN = [
        'CHMI', 'CHML', 'CHMV'
    ]
    # Standard
    STANDARD = [
        'MA01', 'MA02', 'MA03', 'MA04', 'MA05', 'MA06', 'MA07', 'MA08', 'MA09', 'MA10',
        'MA11', 'MA12', 'MA13', 'MA14', 'MA15', 'MA16', 'MA17', 'MA18', 'MA19', 'MA20',
        'MA21', 'MA22', 'MA23', 'MA24', 'MA25', 'MA26', 'MA27', 'MA28', 'MA29', 'MA30',
        'MA31', 'MA32',
        'MABF', 'MF', 'MU01', 'MU02', 'MU03', 'MU04',
        'MV01', 'MV02', 'MV03', 'MV04', 'MV05', 'MV06', 'MV07', 'MV08', 'MV09', 'MV10',
        'MV11', 'MV12', 'MV13', 'MV14', 'MV15', 'MV16', 'MV17', 'MV18', 'MV19', 'MV20',
        'MV21', 'MV22', 'MV23', 'MV24', 'MV25', 'MV26', 'MV27', 'MV28', 'MV29', 'MV30',
        'MV31', 'MV32'
    ]
    # Service
    SERVICE = [
        'ML01', 'ML02'
    ]
    # Domestic
    AU_DOMESTIC = [
        'MFAU', 'MFAU01', 'MFAU02', 'MFAU03', 'MFAU04'
    ]
    # New Zealand
    NZ_DOMESTIC = [
        'MFNZ', 'MFNZ01', 'MFNZ02', 'MFNZ03', 'MFNZ04',
        'MN01', 'MN02', 'MN03', 'MN04', 'MN05', 'MN06', 'MN07', 'MN08', 'MN09', 'MN10',
        'MN11', 'MN12', 'MN13', 'MN14', 'MN15', 'MN16', 'MN17', 'MN18', 'MN19', 'MN20',
        'MN21', 'MN22', 'MN23', 'MN24', 'MN25', 'MN26', 'MN27', 'MN28', 'MN29', 'MN30',
        'MN31', 'MN32',
    ]
    # MO
    MO = [
        'MO01', 'MO02', 'MO03', 'MO04', 'MO05', 'MO06', 'MO07', 'MO08', 'MO09', 'MO10',
        'MO11', 'MO12', 'MO13', 'MO14', 'MO15', 'MO16', 'MO17', 'MO18', 'MO19', 'MO20',
        'MO21', 'MO22', 'MO23', 'MO24', 'MO25', 'MO26', 'MO27', 'MO28', 'MO29', 'MO30',
        'MO31', 'MO32',
    ]
    # Trans Tasman
    TRANS_TASMAN = [
        'MFTT', 'MFTT01', 'MFTT02', 'MFTT03', 'MFTT04',
        'MT01', 'MT02', 'MT03', 'MT04', 'MT05', 'MT06', 'MT07', 'MT08', 'MT09', 'MT10',
        'MT11', 'MT12', 'MT13', 'MT14', 'MT15', 'MT16', 'MT17', 'MT18', 'MT19', 'MT20',
        'MT21', 'MT22', 'MT23', 'MT24', 'MT25', 'MT26', 'MT27', 'MT28', 'MT29', 'MT30',
        'MT31', 'MT32',
    ]
    # JJP
    JETSTAR_JAPAN = [
        'MJ01', 'MJ02', 'MJ03', 'MJ04', 'MJ05', 'MJ06', 'MJ07', 'MJ08', 'MJ09', 'MJ10',
        'MJ11', 'MJ12', 'MJ13', 'MJ14', 'MJ15', 'MJ16', 'MJ17', 'MJ18', 'MJ19', 'MJ20',
        'MJ21', 'MJ22', 'MJ23', 'MJ24', 'MJ25', 'MJ26', 'MJ27', 'MJ28', 'MJ29', 'MJ30',
        'MJ31', 'MJ32',
    ]
    # 3K
    JESTAR_ASIA = [
        'MS01', 'MS02', 'MS03', 'MS04', 'MS05', 'MS06', 'MS07', 'MS08', 'MS09', 'MS10',
        'MS11', 'MS12', 'MS13', 'MS14', 'MS15', 'MS16', 'MS17', 'MS18', 'MS19', 'MS20',
        'MS21', 'MS22', 'MS23', 'MS24', 'MS25', 'MS26', 'MS27', 'MS28', 'MS29', 'MS30',
        'MS31', 'MS32'
    ]


# functional syntax
Color = Enum('Color', ['RED', 'GREEN', 'BLUE'])