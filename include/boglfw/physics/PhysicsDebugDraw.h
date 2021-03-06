/*
 * PhysicsDebugDraw.h
 *
 *  Created on: Nov 27, 2014
 *      Author: bog
 */
 
#ifndef WITH_BOX2D
#error "Box2D support has not been enabled and this is a requirement for using the PhysicsDebugDraw class"
#endif

#ifndef PHYSICSDEBUGDRAW_H_
#define PHYSICSDEBUGDRAW_H_

#include <Box2D/Common/b2Draw.h>

class PhysicsDebugDraw : public b2Draw {
public:
	PhysicsDebugDraw();
	virtual ~PhysicsDebugDraw();

	/// Draw a closed polygon provided in CCW order.
	virtual void DrawPolygon(const b2Vec2* vertices, int32 vertexCount, const b2Color& color);

	/// Draw a solid closed polygon provided in CCW order.
	virtual void DrawSolidPolygon(const b2Vec2* vertices, int32 vertexCount, const b2Color& color);

	/// Draw a circle.
	virtual void DrawCircle(const b2Vec2& center, float32 radius, const b2Color& color);

	/// Draw a solid circle.
	virtual void DrawSolidCircle(const b2Vec2& center, float32 radius, const b2Vec2& axis, const b2Color& color);

	/// Draw a line segment.
	virtual void DrawSegment(const b2Vec2& p1, const b2Vec2& p2, const b2Color& color);

	/// Draw a transform. Choose your own length scale.
	/// @param xf a transform.
	virtual void DrawTransform(const b2Transform& xf);

	/// Draw a point.
	virtual void DrawPoint(const b2Vec2& p, float32 size, const b2Color& color);
};

#endif /* PHYSICSDEBUGDRAW_H_ */
