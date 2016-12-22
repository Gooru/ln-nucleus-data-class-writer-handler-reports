/**
 * Created by ashish on 13/1/16.
 */
package org.gooru.nucleus.handlers.insights.events.processors.repositories;

/**
 * Following Operations are supported right now.
 * 1. Add question to assessment
 * 2. Copy question to assessment
 * 3. Create Assessment
 * 4. Delete Assessment
 * 5. Fetch Assessment
 * 6. Fetch Collaborator
 * 7. Remove question from Assessment
 * 8. Reorder questions in Assessment
 * 9. Update Assessment
 * 10. Update Collaborator
 * 11. Update Question in Assessment
 * <p>
 * Here is the snippet of the logic that we need to cater to:
 * 1. Add question to assessment
 * 2. Copy question to assessment
 * 3. Create Assessment
 * 4. Delete Assessment
 * Pre Conditions
 * - Assessment should exist and it should not be already deleted
 * - The user should be creator_id of that assessment
 * - The assessment should not be published (publish date should be null)
 * Operation
 * - Mark the assessment as deleted, set the modifier_id as user in content table
 * - If the assessment exists in CULCA table, then do the same thing there
 * <p>
 * 5. Fetch Assessment
 * 6. Fetch Collaborator
 * 7. Remove question from Assessment
 * 8. Reorder questions in Assessment
 * 9. Update Assessment
 * 10. Update Collaborator
 * 11. Update Question in Assessment
 */
