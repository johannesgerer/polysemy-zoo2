{-# LANGUAGE UndecidableInstances #-}

module Polysemy.Zoo.Md5 where

import           Codec.Serialise
import           Crypto.Hash.MD5 as MD5
import qualified Data.ByteString.UTF8 as S8
import           Data.ByteString.Builder as BU
import           Data.Serialize
import qualified Data.Text.Lazy.Encoding as LT
import           Polysemy.Zoo.Prelude
import           Polysemy.Zoo.Utils

newtype Md5Hash = Md5Hash { unMd5Hash :: ByteString }
  deriving (Eq, Ord, Generic, Hashable, Serialise, Serialize)

instance TextShow Md5Hash where
  showb h = "Md5Hash " <> fromText (md5HashToHex h)

instance Show Md5Hash where
  show h = "Md5Hash " <> toS (md5HashToHex h)

md5HashToHex :: Md5Hash -> Text
md5HashToHex = toHex . unMd5Hash

class Md5Hashable a where
  md5Hash :: Maybe Md5Hash -> a -> Md5Hash

md5init :: Md5Hashable a => a -> Md5Hash
md5init = md5Hash Nothing
{-# INLINABLE md5init #-}

illegalUtf8ByteValue :: Word8
illegalUtf8ByteValue = 0xff

viaUtf8 :: Foldable f => (a -> BU.Builder) -> Maybe Md5Hash -> f a -> Md5Hash
viaUtf8 f salt = Md5Hash . MD5.hashlazy . BU.toLazyByteString .
  maybe id ((<>) . BU.byteString . unMd5Hash) salt
  . mconcat . intersperse (BU.word8 illegalUtf8ByteValue) . fmap f . toList
{-# INLINABLE viaUtf8 #-}
                                                           
md5HashWithSaltL :: (a -> LByteString) -> Maybe Md5Hash -> a -> Md5Hash
md5HashWithSaltL f s = Md5Hash . MD5.hashlazy . maybe id ((<>) . toS . unMd5Hash) s . f
{-# INLINABLE md5HashWithSaltL #-}

md5HashWithSalt :: (a -> ByteString) -> Maybe Md5Hash -> a -> Md5Hash
md5HashWithSalt f s = Md5Hash . MD5.hash . maybe id ((<>) . unMd5Hash) s . f
{-# INLINABLE md5HashWithSalt #-}

instance {-# OVERLAPS #-} Md5Hashable (Set String) where
  md5Hash = viaUtf8 stringUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable (Set LText) where
  md5Hash = viaUtf8 $ lazyByteString . LT.encodeUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable (Set Text) where
  md5Hash = viaUtf8 $ byteString . encodeUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable [String] where
  md5Hash = viaUtf8 stringUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable [LText] where
  md5Hash = viaUtf8 $ lazyByteString . LT.encodeUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable [Text] where
  md5Hash = viaUtf8 $ byteString . encodeUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable LText where
  md5Hash = md5HashWithSaltL LT.encodeUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable String where
  md5Hash = md5HashWithSalt S8.fromString
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable Text where
  md5Hash = md5HashWithSalt encodeUtf8
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable LByteString where
  md5Hash = md5HashWithSaltL id
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} Md5Hashable ByteString where
  md5Hash = md5HashWithSalt id
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} (Md5Hashable a, Md5Hashable b, Md5Hashable c, Md5Hashable d, Md5Hashable e, Md5Hashable f)
  => Md5Hashable (a,b,c,d,e,f) where
  md5Hash s (a,b,c,d,e,f) = md5Hash s (a,(b,(c,(d,(e,f)))))
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} (Md5Hashable a, Md5Hashable b, Md5Hashable c, Md5Hashable d, Md5Hashable e)
  => Md5Hashable (a,b,c,d,e) where
  md5Hash s (a,b,c,d,e) = md5Hash s (a,(b,(c,(d,e))))
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} (Md5Hashable a, Md5Hashable b, Md5Hashable c, Md5Hashable d)
  => Md5Hashable (a,b,c,d) where
  md5Hash s (a,b,c,d) = md5Hash s (a,(b,(c,d)))
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} (Md5Hashable a, Md5Hashable b, Md5Hashable c) => Md5Hashable (a,b,c) where
  md5Hash s (a,b,c) = md5Hash s (a,(b,c))
  {-# INLINABLE md5Hash #-}

instance {-# OVERLAPS #-} (Md5Hashable a, Md5Hashable b) => Md5Hashable (a,b) where
  md5Hash s (a,b) = md5Hash (Just $ md5Hash s a) b
  {-# INLINABLE md5Hash #-}
