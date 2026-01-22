package com.streamflow.core.repository;

import com.streamflow.core.model.NotificationLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface NotificationRepository extends MongoRepository<NotificationLog, String> {
    
    // Get most recent notifications
    List<NotificationLog> findTop10ByOrderByTimestampDesc();
    
    // Find by type (INFO, WARN, ERROR)
    List<NotificationLog> findByTypeOrderByTimestampDesc(String type);
    
    // Find by userId
    List<NotificationLog> findByUserIdOrderByTimestampDesc(String userId);
    
    // Find by channel (EMAIL, SMS, PUSH, etc.)
    List<NotificationLog> findByChannelOrderByTimestampDesc(String channel);
    
    // Find by type and userId
    List<NotificationLog> findByTypeAndUserIdOrderByTimestampDesc(String type, String userId);
    
    // Find notifications within a time range
    List<NotificationLog> findByTimestampBetweenOrderByTimestampDesc(
            LocalDateTime start, 
            LocalDateTime end
    );
    
    // Paginated query for history/admin view
    Page<NotificationLog> findAllByOrderByTimestampDesc(Pageable pageable);
    
    // Count by type (useful for metrics)
    long countByType(String type);
}
