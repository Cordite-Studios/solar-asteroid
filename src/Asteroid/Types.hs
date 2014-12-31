{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Asteroid.Types where

import qualified Prelude as P
import Prelude(Eq(..),Enum(..),Ord(..),(.),($),Maybe(..),Either(..),(||),Bool(..),not)

import qualified Data.Int           as I
import qualified Data.Text          as T
import qualified Data.Thyme         as C
import qualified Data.UUID          as U
import qualified Data.ByteString    as B
import qualified Data.ByteString.Lazy as BL
import qualified Network.URI        as UR
import qualified Data.Map.Strict    as M
import qualified Data.Vector        as V
import qualified Data.Ix            as I
import qualified Data.Word          as W
import qualified Control.Monad      as MO
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString.Base64.URL as B64U
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Base16 as B16
import qualified System.Locale as L
import qualified Data.Foldable as F

-- Deriving imports
import qualified Data.Aeson as A
import Data.Aeson((.:),(.=))
import Control.Applicative((<$>),(<*>),(<|>))
import Control.Monad(mzero,return,(=<<))
import Data.Ratio((%),numerator,denominator)
import Text.Read(readMaybe)
import qualified Data.Scientific as SC

-- Begin types
type Key = P.Integer

newtype IndexKey        = IndexKey Key      deriving(P.Show, Eq, Ord, Enum, I.Ix)
newtype TxKey           = TxKey Key         deriving(P.Show, Eq, Ord, Enum, I.Ix)
newtype EntityKey       = EntityKey Key     deriving(P.Show, Eq, Ord, Enum, I.Ix)
newtype PartitionKey    = PartitionKey Key  deriving(P.Show, Eq, Ord, Enum, I.Ix)
newtype AttributeKey    = AttributeKey Key  deriving(P.Show, Eq, Ord, Enum, I.Ix)

data Reference = Reference
    { refPartition  :: {-# UNPACK #-} !PartitionKey
    , refEntity     :: {-# UNPACK #-} !EntityKey
    }
    deriving (Eq,Ord)

data Value  = VRef      {-# UNPACK #-} !Reference
            | VText     {-# UNPACK #-} !T.Text
            | VBool     {-# UNPACK #-} !P.Bool
            | VInt32    {-# UNPACK #-} !I.Int32
            | VInt64    {-# UNPACK #-} !I.Int64
            | VInteger  {-# UNPACK #-} !P.Integer
            | VFloat    {-# UNPACK #-} !P.Float
            | VDouble   {-# UNPACK #-} !P.Double
            | VRatio    {-# UNPACK #-} !P.Rational
            | VInstant  {-# UNPACK #-} !C.UTCTime
            | VDate     {-# UNPACK #-} !C.Day
            | VTime     {-# UNPACK #-} !C.TimeOfDay
            | VUUID     {-# UNPACK #-} !U.UUID
            | VURI      {-# UNPACK #-} !UR.URI
            | VBytes    {-# UNPACK #-} !B.ByteString
            deriving(Eq,Ord)

data Datom = Datom
           { entity :: {-# UNPACK #-} !EntityKey
           , attr   :: {-# UNPACK #-} !AttributeKey
           , val    :: {-# UNPACK #-} !Value
           , tx     :: {-# UNPACK #-} !TxKey
           , action :: {-# UNPACK #-} !Action
           }
           deriving (Eq)

data IndexTree  = Root
                    { rootBranches  :: M.Map PartitionKey IndexTree
                    , latestIdents  :: M.Map PartitionKey EntityKey
                    , prevRoot      :: IndexKey
                    }
                | Branch
                    { datoms    :: V.Vector Datom
                    , branches  :: V.Vector IndexRef
                    , depth     :: W.Word8
                    }
                | Leaf
                    { datoms    :: V.Vector Datom
                    }
                deriving(Eq)

data IndexRef = IndexRef
              { index       :: {-# UNPACK #-} !IndexKey
              , statCount   :: {-# UNPACK #-} !P.Integer
              }
              deriving(P.Show, Eq)

data Action = Assert
            | Redact
            deriving(Eq,Enum,P.Bounded,Ord)

data Cardnality = One
                | Many
                deriving(Eq,Enum,P.Bounded,Ord)

data Persist    = OnlyOne
                | AllAsserted
                | All
                deriving(Eq,Enum,P.Bounded,Ord)

-- Type Class Implementations

-- Keys
instance A.FromJSON TxKey where
    parseJSON v = TxKey <$> parseInteger v
instance A.ToJSON TxKey where
    toJSON (TxKey r) = encodeInteger r

instance A.FromJSON IndexKey where
    parseJSON v = IndexKey <$> parseInteger v
instance A.ToJSON IndexKey where
    toJSON (IndexKey r) = encodeInteger r

instance A.FromJSON EntityKey where
    parseJSON v = EntityKey <$> parseInteger v
instance A.ToJSON EntityKey where
    toJSON (EntityKey r) = encodeInteger r
    
instance A.FromJSON PartitionKey where
    parseJSON v = PartitionKey <$> parseInteger v
instance A.ToJSON PartitionKey where
    toJSON (PartitionKey r) = encodeInteger r

instance A.FromJSON AttributeKey where
    parseJSON v = AttributeKey <$> parseInteger v
instance A.ToJSON AttributeKey where
    toJSON (AttributeKey r) = encodeInteger r

-- Enums
instance A.FromJSON Action where
    parseJSON (A.Number n) = case index of
        0 -> return Redact
        1 -> return Assert
        _ -> mzero
        where
            index :: I.Int
            index = case SC.floatingOrInteger n of
                Left (f :: P.Float) -> P.floor f
                Right i -> i
    parseJSON _ = mzero
instance A.ToJSON Action where
    toJSON Redact = A.Number 0
    toJSON Assert = A.Number 1

instance A.FromJSON Persist where
    parseJSON (A.Number n) = case index of
        1 -> return OnlyOne
        2 -> return AllAsserted
        4 -> return All
        _ -> mzero
        where
            index :: I.Int
            index = case SC.floatingOrInteger n of
                Left (f :: P.Float) -> P.floor f
                Right i -> i
    parseJSON _ = mzero
instance A.ToJSON Persist where
    toJSON OnlyOne      = A.Number 1
    toJSON AllAsserted  = A.Number 2
    toJSON All          = A.Number 4

instance A.FromJSON Cardnality where
    parseJSON (A.Number n) = case index of
        1 -> return One
        2 -> return Many
        _ -> mzero
        where
            index :: I.Int
            index = case SC.floatingOrInteger n of
                Left (f :: P.Float) -> P.floor f
                Right i -> i
    parseJSON _ = mzero
instance A.ToJSON Cardnality where
    toJSON One = A.Number 1
    toJSON Many = A.Number 2

instance A.FromJSON Reference where
    parseJSON json = do
        (partition, entity) <- A.parseJSON json
        return $ Reference partition entity
instance A.ToJSON Reference where
    toJSON Reference{..} = A.toJSON (refPartition, refEntity)

-- Needful for Aeson and our types
instance A.FromJSON B.ByteString where
    parseJSON (A.String s) = return . B64.decodeLenient . TE.encodeUtf8 $ s
    parseJSON _ = mzero
instance A.ToJSON B.ByteString where
   toJSON b = A.String . TE.decodeLatin1 . B64.encode $ b

instance A.FromJSON U.UUID where
    parseJSON (A.String s) = mzero
        <|> do
            -- Try the way that the UUID lib likes
            MO.guard hasDash
            returnJust . U.fromString . T.unpack $ s
        <|> do
            -- We could have failed above for improperly placed dashes.
            -- B16 won't take anything non hexy, so at least take out
            -- dashes if needed.
            let s' = if hasDash
                then T.filter (/= '-') s
                else s
            -- Attempt to decode hex-only input
            let (decoded, bad) = B16.decode . TE.encodeUtf8 $ s'
            MO.guard $ B.length bad == 0
            returnJust . uuidFromStrict $ decoded
        <|> do
            -- Perhaps we could try plain base 64.
            -- First, we should check if we might be using one alphabet or the other.
            -- But we can't be using both.
            MO.guard . not $ urlB64
            returnJust . uuidFromStrict . B64.decodeLenient . TE.encodeUtf8 $ s
        <|> do
            -- Perhaps we could try URL base 64.
            MO.guard . not $ plainB64
            returnJust . uuidFromStrict . B64U.decodeLenient . TE.encodeUtf8 $ s
        where
            hasDash = T.foldl' (\a v -> a || (v == '-')) False s
            plainB64 = T.foldl' (\a v -> a || (v == '/') || (v == '+')) False s
            urlB64 = T.foldl' (\a v -> a || (v == '-') || (v == '_')) False s
            uuidFromStrict = U.fromByteString . BL.fromStrict
    parseJSON _ = mzero
instance A.ToJSON U.UUID where
    toJSON u = A.String . T.pack $ U.toString u

instance A.FromJSON UR.URI where
    parseJSON (A.String uri) = returnJust . UR.parseURI . T.unpack $ uri
    parseJSON _ = mzero
instance A.ToJSON UR.URI where
    toJSON uri = A.String . T.pack $ UR.uriToString P.id uri ""

instance A.FromJSON C.UTCTime where
    parseJSON v = returnJust $ F.foldl' (<|>) mzero
        [ parseThyme v "%Y-%m-%dT%H:%M:%S"
        , parseThyme v "%Y-%m-%dT%H:%M:%S%z"
        , parseThyme v "%Y-%m-%dT%H:%M:%S%Z"
        ]
instance A.ToJSON C.UTCTime where
    toJSON t = A.String . T.pack $ C.formatTime L.defaultTimeLocale "%Y-%m-%dT%H:%M:%S%z" t

instance A.FromJSON C.Day where
    parseJSON v = returnJust $ F.foldl' (<|>) mzero
        [ parseThyme v "%Y-%m-%d"
        , parseThyme v "%m/%d/%y"
        , parseThyme v "%m/%d/%Y"
        ]
instance A.ToJSON C.Day where
    toJSON t = A.String . T.pack $ C.formatTime L.defaultTimeLocale "%Y-%m-%d" t

instance A.FromJSON C.TimeOfDay where
    parseJSON v = returnJust $ F.foldl' (<|>) mzero
        [ parseThyme v "%H:%M:%S"
        , parseThyme v "%H:%M:%S%z"
        , parseThyme v "%H:%M:%S%Z"
        ]
instance A.ToJSON C.TimeOfDay where
    toJSON t = A.String . T.pack $ C.formatTime L.defaultTimeLocale "%H:%M:%S" t

instance A.FromJSON (M.Map PartitionKey IndexTree) where
    parseJSON = P.fmap M.fromList . A.parseJSON 
instance A.ToJSON (M.Map PartitionKey IndexTree) where
    toJSON m = A.toJSON $ M.toList m
instance A.FromJSON (M.Map PartitionKey EntityKey) where
    parseJSON = P.fmap M.fromList . A.parseJSON 
instance A.ToJSON (M.Map PartitionKey EntityKey) where
    toJSON m = A.toJSON $ M.toList m
-- Helper functions
returnJust :: MO.MonadPlus m => Maybe a -> m a
returnJust x = case x of
    Nothing -> mzero
    Just x' -> return x'

parseInteger :: (MO.MonadPlus m) => A.Value -> m P.Integer
parseInteger (A.String n) = case readMaybe (T.unpack n) of
    Just n' -> return n'
    _       -> mzero
parseInteger (A.Number n) = case SC.floatingOrInteger n of
    Left (n'::P.Double)  -> return $ P.floor n'
    Right n' -> return n'
parseInteger _ = mzero

encodeInteger :: P.Integer -> A.Value
encodeInteger r = if r < minI || r > maxI
        then A.String $ T.pack $ P.show r
        else A.Number $ SC.scientific r 0
        where
            minI = P.toInteger (P.minBound::I.Int)
            maxI = P.toInteger (P.maxBound::I.Int)

parseRational :: (MO.MonadPlus m) => (A.Value, A.Value) -> m P.Rational
parseRational (n1, n2) = do
    n1' <- parseInteger n1
    n2' <- parseInteger n2
    MO.guard $ n2' /= 0
    return $ n1' % n2'
encodeRational :: P.Rational -> A.Value
encodeRational r = A.toJSON
    [ encodeInteger $ numerator r
    , encodeInteger $ denominator r
    ]

giveValue :: A.ToJSON a => T.Text -> a -> A.Value
giveValue kind val = A.object
    [ "kind" .= kind
    , "val" .= val
    ]

parseThyme :: C.ParseTime t => A.Value -> P.String -> Maybe t
parseThyme (A.String t) f = C.parseTime L.defaultTimeLocale f $ T.unpack t
parseThyme _ _ = mzero

-- Finally the values and such
instance A.FromJSON Value where
    parseJSON (A.Object v) = do
        kind <- v .: "kind"
        case kind of
            A.String "ref" -> VRef <$> v .: val
            A.String "text" -> VText <$> v .: val
            A.String "bool" -> VBool <$> v .: val
            A.String "int32" -> VInt32 <$> v .: val
            A.String "int64" -> VInt64 <$> v .: val
            A.String "int" ->  VInteger <$> (parseInteger =<< v .: val)
            A.String "float" -> VFloat <$> v .: val
            A.String "double" -> VDouble <$> v .: val
            A.String "ratio" -> VRatio <$> (parseRational =<< v .: val)
            A.String "instant" -> VInstant <$> v .: val
            A.String "date" -> VDate <$> v .: val
            A.String "time" -> VTime <$> v .: val
            A.String "uuid" -> VUUID <$> v .: val
            A.String "uri" -> VURI <$> v .: val
            A.String "bytes" -> VBytes <$> v .: val
            _ -> mzero
        where
            val = "val"
    parseJSON _ = mzero
instance A.ToJSON Value where
    toJSON (VRef r)     = giveValue "ref" r
    toJSON (VText r)    = giveValue "text" r
    toJSON (VBool r)    = giveValue "bool" r
    toJSON (VInt32 r)   = giveValue "int32" r
    toJSON (VInt64 r)   = giveValue "int64" r
    toJSON (VInteger r) = giveValue "int" $ encodeInteger r
    toJSON (VFloat r)   = giveValue "float" r
    toJSON (VDouble r)  = giveValue "double" r
    toJSON (VRatio r)   = giveValue "ratio" $ encodeRational r
    toJSON (VInstant r) = giveValue "instant" r
    toJSON (VDate r)    = giveValue "date" r
    toJSON (VTime r)    = giveValue "time" r
    toJSON (VUUID r)    = giveValue "uuid" r
    toJSON (VURI r)     = giveValue "uri" r
    toJSON (VBytes r)   = giveValue "bytes" r

instance A.FromJSON Datom where
    parseJSON (A.Object v) = Datom
        <$> v .: "entity"
        <*> v .: "attr"
        <*> v .: "val"
        <*> v .: "tx"
        <*> v .: "action"
    parseJSON _ = mzero
instance A.ToJSON Datom where
    toJSON Datom{..} = A.object
        [ "entity"  .= entity
        , "attr"    .= attr
        , "val"     .= val
        , "tx"      .= tx
        , "action"  .= action
        ]

instance A.FromJSON IndexRef where
    parseJSON (A.Object v) = IndexRef
        <$> v .: "index"
        <*> v .: "count"
    parseJSON _ = mzero
instance A.ToJSON IndexRef where
    toJSON IndexRef{..} = A.object
        [ "index"   .= index
        , "count"   .= statCount
        ]

instance A.FromJSON IndexTree where
    parseJSON (A.Object v) = mzero
        <|> Root
            <$> v .: "branches"
            <*> v .: "idents"
            <*> v .: "prev"
        <|> Branch
            <$> v .: "datoms"
            <*> v .: "branches"
            <*> v .: "depth"
        <|> Leaf
            <$> v .: "datoms"
    parseJSON _ = mzero
instance A.ToJSON IndexTree where
    toJSON Root{..} = A.object
        [ "branches"    .= rootBranches
        , "idents"      .= latestIdents
        , "prev"        .= prevRoot
        ]
    toJSON Branch{..} = A.object
        [ "datoms"  .= datoms
        , "branches".= branches
        , "depth"   .= depth
        ]
    toJSON Leaf{..} = A.object
        [ "datoms"  .= datoms
        ]
-- Keep space after the source


