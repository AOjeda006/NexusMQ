#include "protocol/error_code.hpp"

#include <gtest/gtest.h>

#include "common/error.hpp"

namespace {

TEST(WireError, IsRetryable_TransitoriosSiPermanentesNo) {
    EXPECT_TRUE(nexus::is_retryable(nexus::WireError::NotLeaderForPartition));
    EXPECT_TRUE(nexus::is_retryable(nexus::WireError::LeaderNotAvailable));
    EXPECT_TRUE(nexus::is_retryable(nexus::WireError::RequestTimedOut));
    EXPECT_TRUE(nexus::is_retryable(nexus::WireError::Throttled));
    EXPECT_TRUE(nexus::is_retryable(nexus::WireError::RebalanceInProgress));

    EXPECT_FALSE(nexus::is_retryable(nexus::WireError::None));
    EXPECT_FALSE(nexus::is_retryable(nexus::WireError::OffsetOutOfRange));
    EXPECT_FALSE(nexus::is_retryable(nexus::WireError::CorruptMessage));
    EXPECT_FALSE(nexus::is_retryable(nexus::WireError::Unauthorized));
}

TEST(WireError, FromError_TraduceLosCodigosDelNucleo) {
    using nexus::Error;
    using nexus::ErrorCode;
    using nexus::WireError;
    EXPECT_EQ(nexus::from_error(Error{ErrorCode::Corrupt, ""}), WireError::CorruptMessage);
    EXPECT_EQ(nexus::from_error(Error{ErrorCode::OutOfRange, ""}), WireError::OffsetOutOfRange);
    EXPECT_EQ(nexus::from_error(Error{ErrorCode::NotFound, ""}),
              WireError::UnknownTopicOrPartition);
    EXPECT_EQ(nexus::from_error(Error{ErrorCode::Unsupported, ""}), WireError::UnsupportedVersion);
    EXPECT_EQ(nexus::from_error(Error{ErrorCode::InvalidArgument, ""}), WireError::InvalidRequest);
    EXPECT_EQ(nexus::from_error(Error{ErrorCode::IoError, ""}), WireError::LeaderNotAvailable);
    EXPECT_EQ(nexus::from_error(Error{ErrorCode::Shutdown, ""}), WireError::LeaderNotAvailable);
}

}  // namespace
