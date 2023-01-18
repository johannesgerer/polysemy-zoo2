{-# LANGUAGE UndecidableInstances #-}

module Polysemy.Zoo.Md5 where

import           Codec.Serialise
import           Crypto.Hash.MD5 as MD5
import           Data.ByteString.Builder as BU
import           Data.Serialize
import qualified Data.Text.Lazy.Encoding as LT
import           Polysemy.Zoo.Prelude
import           Polysemy.Zoo.Utils

newtype Md5Hash = Md5Hash { unMd5Hash :: ByteString }
  deriving (Show, Eq, Ord, Generic, Hashable, Serialise, Serialize)

md5HashToHex :: Md5Hash -> Text
md5HashToHex = toHex . unMd5Hash

class Md5Hashable a where
  md5Hash :: Maybe Md5Hash -> a -> Md5Hash

md5init :: Md5Hashable a => a -> Md5Hash
md5init = md5Hash Nothing
{-# INLINABLE md5init #-}

illegalUtf8ByteValue :: Word8
illegalUtf8ByteValue = 0xff

viaUtf8 :: (a -> BU.Builder) -> Maybe Md5Hash -> [a] -> Md5Hash
viaUtf8 f salt = Md5Hash . MD5.hashlazy . BU.toLazyByteString .
  maybe id ((<>) . BU.byteString . unMd5Hash) salt . mconcat . intersperse (BU.word8 illegalUtf8ByteValue) . fmap f
{-# INLINABLE viaUtf8 #-}
                                                           
md5HashWithSalt :: (a -> ByteString) -> Maybe Md5Hash -> a -> Md5Hash
md5HashWithSalt f s = Md5Hash . MD5.hash . maybe id ((<>) . unMd5Hash) s . f
{-# INLINABLE md5HashWithSalt #-}

instance {-# OVERLAPS #-} Md5Hashable [String] where
  md5Hash = viaUtf8 stringUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable [LText] where
  md5Hash = viaUtf8 $ lazyByteString . LT.encodeUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable [Text] where
  md5Hash = viaUtf8 $ byteString . encodeUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable Text where
  md5Hash = md5HashWithSalt encodeUtf8
  {-# INLINABLE md5Hash #-}

instance ConvertText a Text => Md5Hashable a where
  md5Hash s = md5Hash @Text s . toS
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable ByteString where
  md5Hash = md5HashWithSalt id
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} (Md5Hashable a, Md5Hashable b) => Md5Hashable (a,b) where
  md5Hash s (a,b) = md5Hash (Just $ md5Hash s a) b
  {-# INLINABLE md5Hash #-}
